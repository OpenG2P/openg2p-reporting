package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;


public class RenameTopic<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String PURPOSE = "Change Topic name tranform";
    public static final String TOPIC_CONFIG = "topic";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TOPIC_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "New topic name .");

    private String renamedTopic;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        renamedTopic = absconf.getString(TOPIC_CONFIG);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close(){}

    @Override
    public R apply(R record) {
        return record.newRecord(renamedTopic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
    }
}
