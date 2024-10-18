package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


public class DynamicNewFieldInsertBackTest {
    private final DynamicNewFieldInsertBack<SourceRecord> transform = new DynamicNewFieldInsertBack.Value<>();

    @AfterEach
    public void teardown() {
        transform.close();
    }

    @Test
    public void testSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(DynamicNewFieldInsertBack.ES_URL_CONFIG, "http://localhost:9200");
        config.put(DynamicNewFieldInsertBack.ES_INDEX_CONFIG, "res_partner");
        config.put(DynamicNewFieldInsertBack.ID_EXPR_CONFIG, ".partner_id");
        config.put(DynamicNewFieldInsertBack.CONDITION_EXPR_CONFIG, ".id_type_name == \"NATIONAL ID\"");
        config.put(DynamicNewFieldInsertBack.VALUE_EXPR_CONFIG, "{reg_id_NATIONAL_ID: .value}");

        transform.configure(config);

        Map<String, Object> input = new HashMap<>();
        input.put("partner_id", 12);
        input.put("id_type_name", "NATIONAL ID");
        input.put("value", "1566711");

        transform.apply(createRecordSchemaless(input));
        // TODO: mock ES query
    }

    private SourceRecord createRecordWithSchema(Schema schema, Object value) {
        return new SourceRecord(null, null, "topic", 0, schema, value);
    }

    private SourceRecord createRecordSchemaless(Object value) {
        return createRecordWithSchema(null, value);
    }
}
