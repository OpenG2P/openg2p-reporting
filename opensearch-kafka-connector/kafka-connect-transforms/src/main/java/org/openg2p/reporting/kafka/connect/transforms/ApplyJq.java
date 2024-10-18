package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import org.openg2p.reporting.kafka.connect.transforms.util.JqUtil;

public abstract class ApplyJq<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    public static class Key<R extends ConnectRecord<R>> extends ApplyJq<R>{}
    public static class Value<R extends ConnectRecord<R>> extends ApplyJq<R>{}

    public static enum ErrorBehavior{
        halt,
        ignore,
    }

    private static final Logger logger = LoggerFactory.getLogger(ApplyJq.class);

    public static final String PURPOSE = "Apply Jq transformation";
    public static final String JQ_EXPR_CONFIG = "expr";
    public static final String ERROR_BEHAVIOR_CONFIG = "behavior.on.error";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(JQ_EXPR_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Jq expression to be applied.")
        .define(ERROR_BEHAVIOR_CONFIG, ConfigDef.Type.STRING, "halt", ConfigDef.Importance.HIGH, "What to do when encountering error applying Jq expression. Possible values: \"halt\", \"ignore\"");

    private JqUtil jqUtil;
    private ErrorBehavior errorBehavior;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        String jqExpression = absconf.getString(JQ_EXPR_CONFIG);
        errorBehavior = ErrorBehavior.valueOf(absconf.getString(ERROR_BEHAVIOR_CONFIG));

        if(jqExpression==null || jqExpression.isEmpty()){
            throw new ConfigException(JQ_EXPR_CONFIG + " is not set.");
        }

        try{
            jqUtil = new JqUtil(jqExpression);
        } catch (Exception e) {
            throw new ConfigException("Invalid JQ expression.", e);
        }
    }

    @Override
    public R applySchemaless(R record) {
        try {
            Object output = jqUtil.first(operatingValue(record));
            return newRecord(record, null, output);
        } catch (Exception e) {
            if (errorBehavior == ErrorBehavior.halt) {
                throw new DataException("Jq apply error.", e);
            // } else if (errorBehavior == ErrorBehavior.ignore) {
            } else {
                logger.error("Jq apply error.", e);
                return record;
            }
        }
    }

    @Override
    public R applyWithSchema(R record) {
        throw new DataException("Jq Transform with Schema is not yet supported.");
    }
}
