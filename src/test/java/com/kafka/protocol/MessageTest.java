package com.kafka.protocol;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Message class
 */
public class MessageTest {

    @Test
    public void testMessageCreation() {
        String topic = "test-topic";
        int partition = 0;
        long offset = 123L;
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        Message message = new Message(topic, partition, offset, key, value);

        assertEquals(topic, message.getTopic());
        assertEquals(partition, message.getPartition());
        assertEquals(offset, message.getOffset());
        assertArrayEquals(key, message.getKey());
        assertArrayEquals(value, message.getValue());
        assertTrue(message.getTimestamp() > 0);
        assertTrue(message.getCrc32() != 0);
    }

    @Test
    public void testMessageSerialization() {
        String topic = "test-topic";
        int partition = 1;
        long offset = 456L;
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        Message originalMessage = new Message(topic, partition, offset, key, value);
        byte[] serialized = originalMessage.serialize();
        
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        Message deserializedMessage = Message.deserialize(serialized);

        assertEquals(originalMessage.getTopic(), deserializedMessage.getTopic());
        assertEquals(originalMessage.getPartition(), deserializedMessage.getPartition());
        assertEquals(originalMessage.getOffset(), deserializedMessage.getOffset());
        assertArrayEquals(originalMessage.getKey(), deserializedMessage.getKey());
        assertArrayEquals(originalMessage.getValue(), deserializedMessage.getValue());
        assertEquals(originalMessage.getTimestamp(), deserializedMessage.getTimestamp());
        assertEquals(originalMessage.getCrc32(), deserializedMessage.getCrc32());
    }

    @Test
    public void testMessageWithNullKey() {
        String topic = "test-topic";
        int partition = 0;
        long offset = 0L;
        byte[] value = "test-value".getBytes();

        Message message = new Message(topic, partition, offset, null, value);

        assertNull(message.getKey());
        assertArrayEquals(value, message.getValue());
        assertTrue(message.isValid());
    }

    @Test
    public void testMessageValidation() {
        String topic = "test-topic";
        int partition = 0;
        long offset = 0L;
        byte[] value = "test-value".getBytes();

        Message message = new Message(topic, partition, offset, null, value);
        assertTrue(message.isValid());
    }

    @Test
    public void testMessageEquality() {
        String topic = "test-topic";
        int partition = 0;
        long offset = 0L;
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        Message message1 = new Message(topic, partition, offset, key, value);
        Message message2 = new Message(topic, partition, offset, key, value);

        assertEquals(message1, message2);
        assertEquals(message1.hashCode(), message2.hashCode());
    }
}
