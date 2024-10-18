package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.openg2p.reporting.kafka.connect.transforms.util.JqUtil;


public class ApplyJqTest {
    private final ApplyJq<SourceRecord> applyJqTransform = new ApplyJq.Value<>();
    private final JsonConverter jsonConverter = new JsonConverter();

    @AfterEach
    public void teardown() {
        applyJqTransform.close();
        jsonConverter.close();
    }

    @Test
    public void testSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(ApplyJq.JQ_EXPR_CONFIG, ".source * .after * .report");

        applyJqTransform.configure(config);

        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("converter.type", "value");
        jsonConverter.configure(jsonConverterConfig);

        SchemaAndValue original = jsonConverter.toConnectData(
            "topic",
            ("{" +
            "    \"after\": {" +
            "        \"finalize\": {" +
            "            \"test2\":\"val2\"," +
            "            \"preamble\":null" +
            "        }," +
            "        \"array\":[\"list2\"]" +
            "    }," +
            "    \"source\": {" +
            "        \"finalize\":{" +
            "            \"test1\":\"val1\"," +
            "            \"preamble\":\"fine\"" +
            "        }," +
            "        \"array\":[\"list1\"]" +
            "    }," +
            "    \"report\": {" +
            "        \"finalize\":{" +
            "            \"test3\":\"val3\"," +
            "            \"test2\":\"present\"" +
            "        }," +
            "        \"array\":[\"list3\"]" +
            "    }" +
            "}").getBytes()
        );

        SourceRecord transformed = applyJqTransform.apply(createTestRecord(original));

        Map<String, Object> transformedFinalized = (Map<String, Object>)((Map<String, Object>)transformed.value()).get("finalize");
        List<String> transformedFinalizedArray = (List<String>)((Map<String, Object>)transformed.value()).get("array");
        assertNull(transformedFinalized.get("preamble"));
        assertEquals("val1", transformedFinalized.get("test1"));
        assertEquals("present", transformedFinalized.get("test2"));
        assertEquals("val3", transformedFinalized.get("test3"));

        // Merges Maps but doesnt merge arrays.
        assertEquals("list3", transformedFinalizedArray.get(0));
    }

    @Test
    public void testJqUtil() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put("test1", "val1");
        Map<String, Object> map2 = new HashMap<>();
        map2.put("test21","val2");
        map2.put("test22",123);
        map1.put("test2", map2);

        Object output = null;
        Map<String, Object> outputMap = null;
        try{
            output = new JqUtil(".test2").first(map1);
            outputMap = (Map<String, Object>) output;
        } catch(Exception e){
            System.out.println("Unexpected Error while JqUtil Test.");
            e.printStackTrace();
        }

        assertTrue(output instanceof Map);
        assertEquals("val2", outputMap.get("test21"));
        assertEquals(123, outputMap.get("test22"));
    }

    private SourceRecord createTestRecord(SchemaAndValue schemaAndValue) {
        return new SourceRecord(null, null, "topic", 0, schemaAndValue.schema(), schemaAndValue.value());
    }
}
