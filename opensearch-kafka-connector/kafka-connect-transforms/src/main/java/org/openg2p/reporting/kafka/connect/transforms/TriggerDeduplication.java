package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.Requirements;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.openg2p.reporting.kafka.connect.transforms.util.JqUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class TriggerDeduplication<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    public static class Key<R extends ConnectRecord<R>> extends TriggerDeduplication<R>{}
    public static class Value<R extends ConnectRecord<R>> extends TriggerDeduplication<R>{}

    private static final Logger logger = LoggerFactory.getLogger(TriggerDeduplication.class);

    public class Config{
        JqUtil idExpr;
        String dedupeServiceBaseUrl;
        String configName;
        JqUtil beforeExpr;
        JqUtil afterExpr;
        int waitBeforeExecSecs;

        CloseableHttpClient hClient;

        Config(String dedupeServiceBaseUrl, String configName, JqUtil idExpr, JqUtil beforeExpr, JqUtil afterExpr, int waitBeforeExecSecs){
            this.dedupeServiceBaseUrl = dedupeServiceBaseUrl;
            this.configName = configName;
            this.idExpr = idExpr;
            this.beforeExpr = beforeExpr;
            this.afterExpr = afterExpr;
            this.waitBeforeExecSecs = waitBeforeExecSecs;

            hClient = HttpClients.custom().build();
        }

        void triggerDeduplication(Object value){
            HttpGet getConfigGet = new HttpGet(dedupeServiceBaseUrl+"/config/"+configName);
            JsonNode configRes = null;
            try(CloseableHttpResponse hResponse = hClient.execute(getConfigGet)) {
                String hResString = EntityUtils.toString(hResponse.getEntity());
                configRes = idExpr.getObjectMapper().readTree(hResString);
                if (hResponse.getCode() >= 400) {
                    throw new IOException("Http Status Code: " + hResponse.getCode() + " " + hResString);
                }
            } catch(Exception e) {
                throw new DataException("Error reading config.", e);
            }

            if (beforeExpr != null) {
                Map<String, Object> beforeValues = null;
                Map<String, Object> afterValues = null;
                try {
                    beforeValues = beforeExpr.first(value, HashMap.class);
                } catch(Exception e) {
                    throw new DataException("Error evaluating before expression.", e);
                }
                try {
                    afterValues = afterExpr.first(value, HashMap.class);
                } catch(Exception e) {
                    throw new DataException("Error evaluating after expression.", e);
                }
                boolean objectChanged = false;
                for (JsonNode config: configRes.get("config").get("fields")){
                    Object beforeFieldValue = beforeValues.get(config.get("name").asText());
                    Object afterFieldValue = afterValues.get(config.get("name").asText());
                    System.out.println("BEFORE VALUE. AFTER VALUE. FIELD NAME : " + beforeFieldValue + afterFieldValue + config.get("name").asText());
                    if(!afterFieldValue.equals(beforeFieldValue)){
                        objectChanged = true;
                        break;
                    }
                }
                if (objectChanged) {
                    Object idValue = null;
                    try {
                        idValue = idExpr.first(value);
                    } catch(Exception e) {
                        throw new DataException("Error evaluating ID expression.", e);
                    }
                    HttpPost triggerDedupePost = new HttpPost(dedupeServiceBaseUrl+"/deduplicate");
                    triggerDedupePost.setHeader("Content-type", "application/json");

                    Map<String, Object> reqMap = new HashMap<>();
                    reqMap.put("doc_id", idValue);
                    reqMap.put("dedupe_config_name", configName);
                    reqMap.put("wait_before_exec_secs", waitBeforeExecSecs);
                    String reqJson = null;
                    try {
                        reqJson = idExpr.getObjectMapper().writeValueAsString(reqMap);
                    } catch(Exception e) {
                        throw new DataException("Error creating dedupe request.", e);
                    }
                    triggerDedupePost.setEntity(new StringEntity(reqJson));
                    try(CloseableHttpResponse hResponse = hClient.execute(triggerDedupePost)) {
                        String triggerRes = EntityUtils.toString(hResponse.getEntity());
                        if (hResponse.getCode() >= 400) {
                            throw new IOException("Http Status Code: " + hResponse.getCode() + " " + triggerRes);
                        }
                    } catch(Exception e) {
                        throw new DataException("Error triggering dedupe.", e);
                    }
                } else {
                    logger.info("No Change detected for triggering dedupe.");
                }
            }
        }
    }

    public static final String PURPOSE = "trigger deduplication";

    public static final String DEDUPE_BASE_URL_CONFIG = "deduplication.base.url";
    public static final String DEDUPE_CONFIG_NAME = "dedupe.config.name";
    public static final String ID_EXPR_CONFIG = "id.expr";
    public static final String BEFORE_EXPR_CONFIG = "before.expr";
    public static final String AFTER_EXPR_CONFIG = "after.expr";
    public static final String WAIT_BEFORE_EXEC_SECS = "wait.before.exec.secs";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(DEDUPE_BASE_URL_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Base URL of deduplication service")
        .define(DEDUPE_CONFIG_NAME, ConfigDef.Type.STRING, "default", ConfigDef.Importance.HIGH, "Deduplication config name")
        .define(ID_EXPR_CONFIG, ConfigDef.Type.STRING, ".payload.after.id | tostring", ConfigDef.Importance.HIGH, "Jq expr for retrieving the id of the document to be deduplicated")
        .define(BEFORE_EXPR_CONFIG, ConfigDef.Type.STRING, ".payload.before", ConfigDef.Importance.HIGH, "Jq expr for retrieving the before part of payload")
        .define(AFTER_EXPR_CONFIG, ConfigDef.Type.STRING, ".payload.after", ConfigDef.Importance.HIGH, "Jq expr for retrieving the after part of payload")
        .define(WAIT_BEFORE_EXEC_SECS, ConfigDef.Type.INT, 10, ConfigDef.Importance.HIGH, "Time in seconds to wait before starting deduplication");

    private Config config;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        String dedupeServiceBaseUrl = absconf.getString(DEDUPE_BASE_URL_CONFIG);
        String configName = absconf.getString(DEDUPE_CONFIG_NAME);
        String idExprString = absconf.getString(ID_EXPR_CONFIG);
        String beforeExprString = absconf.getString(BEFORE_EXPR_CONFIG);
        String afterExprString = absconf.getString(AFTER_EXPR_CONFIG);
        int waitBeforeExecSecs = absconf.getInt(WAIT_BEFORE_EXEC_SECS);

        JqUtil idExpr = null;
        JqUtil beforeExpr = null;
        JqUtil afterExpr = null;

        try {
            idExpr = new JqUtil(idExprString);
        } catch(Exception e) {
            throw new ConfigException("Invalid Jq expr for ID: " + e.getMessage(), e);
        }

        if (!beforeExprString.isEmpty()){
            try {
                beforeExpr = new JqUtil(beforeExprString);
            } catch(Exception e) {
                throw new ConfigException("Invalid Jq expr for before: " + e.getMessage(), e);
            }
        }

        if(!afterExprString.isEmpty()){
            try {
                afterExpr = new JqUtil(afterExprString);
            } catch(Exception e) {
                throw new ConfigException("Invalid Jq expr for after: " + e.getMessage(), e);
            }
        }

        config = new Config(dedupeServiceBaseUrl, configName, idExpr, beforeExpr, afterExpr, waitBeforeExecSecs);
    }

    @Override
    public R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMap(operatingValue(record), PURPOSE);

        config.triggerDeduplication(value);

        return record;
    }

    @Override
    public R applyWithSchema(R record) {
        throw new DataException("TriggerDeduplication transform with schema is not yet supported.");
    }
}
