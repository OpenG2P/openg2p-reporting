package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class RenameTopicTest {
    private final RenameTopic<SourceRecord> RenameTopicTransform = new RenameTopic<>();

    @AfterEach
    public void teardown() {
        RenameTopicTransform.close();
    }

    @Test
    public void testRenameTopic() {
        Map<String, String> config = new HashMap<>();
        config.put(RenameTopic.TOPIC_CONFIG, "new_topic");

        RenameTopicTransform.configure(config);

        SourceRecord original = new SourceRecord(null, null, "topic", 0, null, "value");

        SourceRecord transformed = RenameTopicTransform.apply(original);

        assertEquals("new_topic", transformed.topic());
    }
}
