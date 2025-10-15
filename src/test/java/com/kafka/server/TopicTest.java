package com.kafka.server;

import com.kafka.protocol.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Topic class
 */
public class TopicTest {

    private Topic topic;

    @BeforeEach
    public void setUp() {
        topic = new Topic("test-topic", 3);
    }

    @Test
    public void testTopicCreation() {
        assertEquals("test-topic", topic.getName());
        assertEquals(3, topic.getPartitionCount());
        assertTrue(topic.getCreatedAt() > 0);
    }

    @Test
    public void testProduceMessage() {
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();
        Message message = new Message("test-topic", 0, 0, key, value);

        long offset = topic.produceMessage(message);

        assertEquals(0, offset);
        assertEquals(1, topic.getMessageCount(0));
        assertEquals(0, topic.getLatestOffset(0));
    }

    @Test
    public void testProduceMessageToDifferentPartitions() {
        byte[] value = "test-value".getBytes();

        // Produce to partition 0
        Message message0 = new Message("test-topic", 0, 0, null, value);
        long offset0 = topic.produceMessage(message0);

        // Produce to partition 1
        Message message1 = new Message("test-topic", 1, 0, null, value);
        long offset1 = topic.produceMessage(message1);

        assertEquals(0, offset0);
        assertEquals(0, offset1);
        assertEquals(1, topic.getMessageCount(0));
        assertEquals(1, topic.getMessageCount(1));
        assertEquals(0, topic.getMessageCount(2));
    }

    @Test
    public void testFetchMessages() {
        byte[] value = "test-value".getBytes();
        Message message = new Message("test-topic", 0, 0, null, value);
        topic.produceMessage(message);

        var messages = topic.fetchMessages(0, 0, 10);

        assertEquals(1, messages.size());
        assertEquals("test-value", new String(messages.get(0).getValue()));
    }

    @Test
    public void testFetchMessagesWithOffset() {
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        Message message1 = new Message("test-topic", 0, 0, null, value1);
        Message message2 = new Message("test-topic", 0, 0, null, value2);
        
        topic.produceMessage(message1);
        topic.produceMessage(message2);

        var messages = topic.fetchMessages(0, 1, 10);

        assertEquals(1, messages.size());
        assertEquals("value2", new String(messages.get(0).getValue()));
    }

    @Test
    public void testInvalidPartition() {
        byte[] value = "test-value".getBytes();
        Message message = new Message("test-topic", 5, 0, null, value);

        assertThrows(IllegalArgumentException.class, () -> {
            topic.produceMessage(message);
        });
    }

    @Test
    public void testGetOffsets() {
        byte[] value = "test-value".getBytes();
        Message message = new Message("test-topic", 0, 0, null, value);
        topic.produceMessage(message);

        assertEquals(0, topic.getEarliestOffset(0));
        assertEquals(0, topic.getLatestOffset(0));
    }
}
