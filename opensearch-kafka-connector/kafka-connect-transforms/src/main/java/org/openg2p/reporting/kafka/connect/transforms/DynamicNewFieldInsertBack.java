package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.Requirements;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.ssl.SSLContextBuilder;

import java.util.Base64;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.util.Map;

import org.openg2p.reporting.kafka.connect.transforms.util.JqUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


public abstract class DynamicNewFieldInsertBack<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    public static class Key<R extends ConnectRecord<R>> extends DynamicNewFieldInsertBack<R>{}
    public static class Value<R extends ConnectRecord<R>> extends DynamicNewFieldInsertBack<R>{}

    private static final Logger logger = LoggerFactory.getLogger(ApplyJq.class);

    public abstract class Config{
        String type;
        JqUtil idExpr;
        JqUtil conditionExpr;
        JqUtil valueExpr;

        Config(String type, JqUtil idExpr, JqUtil conditionExpr, JqUtil valueExpr){
            this.type = type;
            this.idExpr = idExpr;
            this.conditionExpr = conditionExpr;
            this.valueExpr = valueExpr;
        }

        abstract void insertBack(Object input);

        abstract void close();
    }

    public class ESQueryConfig extends Config{
        CloseableHttpClient hClient;
        String esUrl;
        String esIndex;
        boolean esSecurity;
        String esUsername;
        String esPassword;

        ESQueryConfig(
            String type,
            JqUtil idExpr,
            JqUtil conditionExpr,
            JqUtil valueExpr,
            String esUrl,
            String esIndex,
            String esSecurity,
            String esUsername,
            String esPassword
        ) {
            super(type,idExpr,conditionExpr,valueExpr);

            this.esUrl = esUrl;
            this.esIndex = esIndex;
            this.esSecurity = false;
            this.esUsername = esUsername;
            this.esPassword = esPassword;

            HttpClientBuilder hClientBuilder = HttpClients.custom();
            if(esSecurity!=null && !esSecurity.isEmpty() && "true".equals(esSecurity)) {
                this.esSecurity = true;
                try{
                    hClientBuilder.setConnectionManager(
                        PoolingHttpClientConnectionManagerBuilder.create().setSSLSocketFactory(
                            SSLConnectionSocketFactoryBuilder.create().setSslContext(
                                SSLContextBuilder.create().loadTrustMaterial(
                                    TrustAllStrategy.INSTANCE
                                ).build()
                            ).setHostnameVerifier(
                                NoopHostnameVerifier.INSTANCE
                            ).build()
                        ).build()
                    );
                } catch(NoSuchAlgorithmException | KeyManagementException | KeyStoreException e ){
                    throw new ConfigException("Cannot Initialize ES Httpclient for security", e);
                }
            }
            hClient = hClientBuilder.build();
        }

        @Override
        void insertBack(Object input){
            JsonNode inputJson = valueExpr.getObjectMapper().valueToTree(input);
            JsonNode condition;
            try {
                condition = conditionExpr.firstRaw(inputJson);
            } catch (Exception e) {
                logger.error("DynamicNewFieldInsertBack: could not render condition expr.", e);
                return;
            }
            if (condition.asBoolean()) {
                String idValue;
                JsonNode value;
                try {
                    idValue = idExpr.firstRaw(inputJson).asText();
                } catch (Exception e) {
                    logger.error("DynamicNewFieldInsertBack: could not render id expr.", e);
                    return;
                }
                try {
                    value = valueExpr.firstRaw(inputJson);
                } catch (Exception e) {
                    logger.error("DynamicNewFieldInsertBack: could not render value expr.", e);
                    return;
                }
                HttpPost hPost = new HttpPost(esUrl + "/" + esIndex + "/_update/" + idValue);
                hPost.setHeader("Content-type", "application/json");
                if(esSecurity) {
                    hPost.setHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((esUsername + ":" + esPassword).getBytes()));
                }

                ObjectNode requestJson = JsonNodeFactory.instance.objectNode();
                requestJson.set("doc", value);
                requestJson.set("doc_as_upsert", BooleanNode.TRUE);
                hPost.setEntity(new StringEntity(requestJson.toString()));
                try(CloseableHttpResponse hResponse = hClient.execute(hPost)){
                    if(hResponse.getCode() >= 200 && hResponse.getCode() < 300){
                        logger.error("DynamicNewFieldInsertBack: Error occured while ES query. " + EntityUtils.toString(hResponse.getEntity()));
                    }
                } catch(Exception e) {
                    logger.error("DynamicNewFieldInsertBack: ES connection issues.", e);
                    return;
                }
            }
        }

        @Override
        void close(){
            try{hClient.close();}catch(Exception e){}
        }
    }

    public static final String PURPOSE = "dynamic field insertion back into external index";
    public static final String TYPE_CONFIG = "query.type";

    // Base Config
    public static final String ID_EXPR_CONFIG = "id.expr";
    public static final String CONDITION_EXPR_CONFIG = "condition";
    public static final String VALUE_EXPR_CONFIG = "value";
    public static final String STOP_AFTER_INSERT_CONFIG = "stop.record.after.insert";

    // Elasticsearch Specific Config
    public static final String ES_URL_CONFIG = "es.url";
    public static final String ES_INDEX_CONFIG = "es.index";
    public static final String ES_SECURITY_ENABLED_CONFIG = "es.security.enabled";
    public static final String ES_USERNAME_CONFIG = "es.username";
    public static final String ES_PASSWORD_CONFIG = "es.password";

    private Config config;
    private boolean stopRecordAfterInsert;

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TYPE_CONFIG, ConfigDef.Type.STRING, "es", ConfigDef.Importance.HIGH, "This is the type of query made. Defaults to es")
        .define(ID_EXPR_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "ID of the external document to be updated. Given as Jq expr.")
        .define(CONDITION_EXPR_CONFIG, ConfigDef.Type.STRING, "true", ConfigDef.Importance.HIGH, "Condition to be evaluated. Only when the condition evaluates to true, the value is inserted back. Given as Jq expr.")
        .define(VALUE_EXPR_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Value to be inserted back into external document. Given as Jq expr.")
        .define(STOP_AFTER_INSERT_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, "Stop the record from moving to next transform.")

        .define(ES_URL_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Installed Elasticsearch URL")
        .define(ES_INDEX_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Name of the index in ES to search")
        .define(ES_SECURITY_ENABLED_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Is Elasticsearch security enabled?")
        .define(ES_USERNAME_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Elasticsearch Username")
        .define(ES_PASSWORD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Elasticsearch Password");


    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        String type = absconf.getString(TYPE_CONFIG);

        String idExprString = absconf.getString(ID_EXPR_CONFIG);
        String conditionExprString = absconf.getString(CONDITION_EXPR_CONFIG);
        String valueExprString = absconf.getString(VALUE_EXPR_CONFIG);
        stopRecordAfterInsert = absconf.getBoolean(STOP_AFTER_INSERT_CONFIG);

        if (type.isEmpty() || idExprString.isEmpty() || conditionExprString.isEmpty() || valueExprString.isEmpty()) {
            throw new ConfigException("One of required config fields not set. Required fields in tranform: " + TYPE_CONFIG + " ," + ID_EXPR_CONFIG + " ," + CONDITION_EXPR_CONFIG + " ," + VALUE_EXPR_CONFIG);
        }

        JqUtil idExpr = null;
        JqUtil conditionExpr = null;
        JqUtil valueExpr = null;

        try {
            idExpr = new JqUtil(idExprString);
            conditionExpr = new JqUtil(conditionExprString);
            valueExpr = new JqUtil(valueExprString);
        } catch (Exception e) {
            throw new ConfigException("Invalid Jq config.", e);
        }

        switch (type) {
            case "es":
                String esUrl = absconf.getString(ES_URL_CONFIG);
                String esIndex = absconf.getString(ES_INDEX_CONFIG);
                String esSecurity = absconf.getString(ES_SECURITY_ENABLED_CONFIG);
                String esUsername = absconf.getString(ES_USERNAME_CONFIG);
                String esPassword = absconf.getString(ES_PASSWORD_CONFIG);

                if(esUrl.isEmpty() || esIndex.isEmpty()){
                    throw new ConfigException("One of required transform Elasticsearch config fields not set. Required Elasticsearch fields in tranform: " + ES_URL_CONFIG + " ," + ES_INDEX_CONFIG);
                }

                try{
                    config = new ESQueryConfig(
                        type,
                        idExpr,
                        conditionExpr,
                        valueExpr,
                        esUrl,
                        esIndex,
                        esSecurity,
                        esUsername,
                        esPassword
                    );
                }
                catch(Exception e){
                    throw new ConfigException("Can't connect to ElasticSearch. Given url : " + esUrl, e);
                }
                break;
            default:
                throw new ConfigException("Unknown Type : " + type + ". Available types: \"es\"" );
        }
    }

    @Override
    public void close() {
        if(config != null) config.close();
    }

    @Override
    public R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMap(operatingValue(record), PURPOSE);

        config.insertBack(value);

        if (stopRecordAfterInsert){
            return null;
        }

        return record;
    }

    @Override
    public R applyWithSchema(R record) {
        throw new DataException("Dynamic New Field Insert Back Transform with Schema is not yet supported.");
    }
}
