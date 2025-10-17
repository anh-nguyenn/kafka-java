package com.kafka.protocol;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Test cases for BatchMessage
 */
public class BatchMessageTest {

    @Test
    public void testBatchMessageCreation() {
        String topic = "test-topic";
        int partition = 0;
        List<Message> messages = Arrays.asList(
            new Message(topic, partition, 0, "key1".getBytes(), "value1".getBytes()),
            new Message(topic, partition, 1, "key2".getBytes(), "value2".getBytes()),
            new Message(topic, partition, 2, "key3".getBytes(), "value3".getBytes())
        );

        BatchMessage batchMessage = new BatchMessage(topic, partition, messages);

        assertEquals(topic, batchMessage.getTopic());
        assertEquals(partition, batchMessage.getPartition());
        assertEquals(3, batchMessage.getMessageCount());
        assertNotNull(batchMessage.getBatchId());
        assertTrue(batchMessage.isValid());
    }

    @Test
    public void testBatchMessageSerialization() throws IOException {
        String topic = "test-topic";
        int partition = 1;
        List<Message> messages = Arrays.asList(
            new Message(topic, partition, 0, "key1".getBytes(), "value1".getBytes()),
            new Message(topic, partition, 1, "key2".getBytes(), "value2".getBytes())
        );

        BatchMessage originalBatch = new BatchMessage(topic, partition, messages);
        byte[] serialized = originalBatch.serialize();
        
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        BatchMessage deserializedBatch = BatchMessage.deserialize(serialized);
        
        assertEquals(originalBatch.getTopic(), deserializedBatch.getTopic());
        assertEquals(originalBatch.getPartition(), deserializedBatch.getPartition());
        assertEquals(originalBatch.getMessageCount(), deserializedBatch.getMessageCount());
        assertEquals(originalBatch.getBatchId(), deserializedBatch.getBatchId());
        assertTrue(deserializedBatch.isValid());
    }

    @Test
    public void testBatchMessageStatistics() {
        String topic = "test-topic";
        int partition = 0;
        List<Message> messages = Arrays.asList(
            new Message(topic, partition, 0, "key1".getBytes(), "value1".getBytes()),
            new Message(topic, partition, 1, "key2".getBytes(), "value2".getBytes()),
            new Message(topic, partition, 2, "key3".getBytes(), "value3".getBytes())
        );

        BatchMessage batchMessage = new BatchMessage(topic, partition, messages);

        int totalSize = batchMessage.getTotalSize();
        assertEquals(18, totalSize); // "value1" + "value2" + "value3" = 6 + 6 + 6 = 18

        double avgSize = batchMessage.getAverageMessageSize();
        assertEquals(6.0, avgSize, 0.001);

        double compressionRatio = batchMessage.getCompressionRatio();
        assertEquals(1.0, compressionRatio, 0.001); // No compression
    }

    @Test
    public void testEmptyBatchMessage() {
        String topic = "test-topic";
        int partition = 0;
        List<Message> messages = Arrays.asList();

        BatchMessage batchMessage = new BatchMessage(topic, partition, messages);

        assertEquals(0, batchMessage.getMessageCount());
        assertEquals(0, batchMessage.getTotalSize());
        assertEquals(0.0, batchMessage.getAverageMessageSize(), 0.001);
        assertTrue(batchMessage.isValid());
    }

    @Test
    public void testBatchMessageWithCompressedMessages() throws IOException {
        String topic = "test-topic";
        int partition = 0;
        
        // Create messages with compression
        Message msg1 = Message.createCompressed(topic, partition, 0, "key1".getBytes(), 
            "This is a long message that should compress well because it has repeated patterns. ".repeat(10).getBytes());
        Message msg2 = Message.createCompressed(topic, partition, 1, "key2".getBytes(), 
            "Another long message with repeated content for compression testing. ".repeat(10).getBytes());
        
        List<Message> messages = Arrays.asList(msg1, msg2);
        BatchMessage batchMessage = new BatchMessage(topic, partition, messages);

        assertTrue(batchMessage.getCompressionRatio() < 1.0, "Compression ratio should be less than 1.0");
        assertTrue(batchMessage.isValid());
    }

    @Test
    public void testBatchMessageEquality() {
        String topic = "test-topic";
        int partition = 0;
        List<Message> messages = Arrays.asList(
            new Message(topic, partition, 0, "key1".getBytes(), "value1".getBytes())
        );

        BatchMessage batch1 = new BatchMessage(topic, partition, messages);
        BatchMessage batch2 = new BatchMessage(topic, partition, messages);

        // Note: These won't be equal due to different timestamps and batch IDs
        assertNotEquals(batch1, batch2);
        assertNotEquals(batch1.hashCode(), batch2.hashCode());
    }

    @Test
    public void testBatchMessageToString() {
        String topic = "test-topic";
        int partition = 2;
        List<Message> messages = Arrays.asList(
            new Message(topic, partition, 0, "key1".getBytes(), "value1".getBytes())
        );

        BatchMessage batchMessage = new BatchMessage(topic, partition, messages);
        String toString = batchMessage.toString();

        assertTrue(toString.contains("test-topic"));
        assertTrue(toString.contains("partition=2"));
        assertTrue(toString.contains("messageCount=1"));
        assertTrue(toString.contains("BatchMessage"));
    }
}
