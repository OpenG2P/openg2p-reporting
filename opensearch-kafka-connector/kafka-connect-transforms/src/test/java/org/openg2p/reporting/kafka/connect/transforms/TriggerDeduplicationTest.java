package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

public class TriggerDeduplicationTest {
    private final TriggerDeduplication<SourceRecord> transform = new TriggerDeduplication.Value<>();

    @AfterEach
    public void teardown() {
        transform.close();
    }

    @Test
    public void testTriggerNoChangeSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(TriggerDeduplication.DEDUPE_BASE_URL_CONFIG, "http://localhost:8000");

        transform.configure(config);

        Map<String, Object> inputAfter = new HashMap<>();
        Map<String, Object> inputBefore = new HashMap<>();
        Map<String, Object> inputPayload = new HashMap<>();
        Map<String, Object> input = new HashMap<>();
        inputBefore.put("id", 123);
        inputBefore.put("name", "Test1 User1");
        inputBefore.put("address", "Street 1");
        inputBefore.put("birthdate", "2000/01/01");
        inputBefore.put("field_random", "val1");

        inputAfter.put("id", 123);
        inputAfter.put("name", "Test1 User1");
        inputAfter.put("address", "Street 1");
        inputAfter.put("birthdate", "2000/01/01");
        inputAfter.put("field_random", "val2");

        inputPayload.put("before", inputBefore);
        inputPayload.put("after", inputAfter);

        input.put("payload", inputPayload);

        transform.apply(createRecordSchemaless(input));
    }

    @Test
    public void testTriggerNormalSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(TriggerDeduplication.DEDUPE_BASE_URL_CONFIG, "http://localhost:8000");

        transform.configure(config);

        Map<String, Object> inputAfter = new HashMap<>();
        Map<String, Object> inputBefore = new HashMap<>();
        Map<String, Object> inputPayload = new HashMap<>();
        Map<String, Object> input = new HashMap<>();
        inputBefore.put("id", 123);
        inputBefore.put("name", "Test1 User1");
        inputBefore.put("address", "Street 1");
        inputBefore.put("birthdate", "2000/01/01");
        inputBefore.put("field_random", "val1");

        inputAfter.put("id", "123");
        inputAfter.put("name", "Test1 User2");
        inputAfter.put("address", "Street 2");
        inputAfter.put("birthdate", "2000/01/01");
        inputAfter.put("field_random", "val2");

        inputPayload.put("before", inputBefore);
        inputPayload.put("after", inputAfter);

        input.put("payload", inputPayload);

        transform.apply(createRecordSchemaless(input));
    }

    private SourceRecord createRecordWithSchema(Schema schema, Object value) {
        return new SourceRecord(null, null, "topic", 0, schema, value);
    }

    private SourceRecord createRecordSchemaless(Object value) {
        return createRecordWithSchema(null, value);
    }
}
