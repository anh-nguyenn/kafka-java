package com.kafka.protocol;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents a message in the Kafka-like broker system.
 * Contains the core message data structure with serialization support.
 */
public class Message {
    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] key;
    private final byte[] value;
    private long timestamp;
    private int crc32;

    public Message(String topic, int partition, long offset, byte[] key, byte[] value) {
        this.topic = Objects.requireNonNull(topic, "Topic cannot be null");
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = Objects.requireNonNull(value, "Value cannot be null");
        this.timestamp = Instant.now().toEpochMilli();
        this.crc32 = calculateCRC32();
    }

    /**
     * Creates a message from serialized bytes
     */
    public static Message deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        // Read header
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        long timestamp = buffer.getLong();
        int crc32 = buffer.getInt();
        
        // Read key
        int keyLength = buffer.getInt();
        byte[] key = null;
        if (keyLength > 0) {
            key = new byte[keyLength];
            buffer.get(key);
        }
        
        // Read value
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        
        Message message = new Message(topic, partition, offset, key, value);
        message.timestamp = timestamp;
        message.crc32 = crc32;
        
        return message;
    }

    /**
     * Serializes the message to bytes
     */
    public byte[] serialize() {
        int keyLength = key != null ? key.length : 0;
        int valueLength = value.length;
        int topicLength = topic.getBytes().length;
        
        // Calculate total size
        int totalSize = 4 + topicLength + 4 + 8 + 8 + 4 + 4 + keyLength + 4 + valueLength;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write header
        buffer.putInt(topicLength);
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putLong(timestamp);
        buffer.putInt(crc32);
        
        // Write key
        buffer.putInt(keyLength);
        if (key != null) {
            buffer.put(key);
        }
        
        // Write value
        buffer.putInt(valueLength);
        buffer.put(value);
        
        return buffer.array();
    }

    /**
     * Calculates CRC32 checksum for data integrity
     */
    private int calculateCRC32() {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(topic.getBytes());
        crc.update(partition);
        crc.update((int) offset);
        crc.update((int) timestamp);
        if (key != null) {
            crc.update(key);
        }
        crc.update(value);
        return (int) crc.getValue();
    }

    /**
     * Validates message integrity using CRC32
     */
    public boolean isValid() {
        return crc32 == calculateCRC32();
    }

    // Getters
    public String getTopic() { return topic; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public long getTimestamp() { return timestamp; }
    public int getCrc32() { return crc32; }

    @Override
    public String toString() {
        return String.format("Message{topic='%s', partition=%d, offset=%d, key=%s, value=%s, timestamp=%d}",
                topic, partition, offset, 
                key != null ? new String(key) : "null",
                new String(value), timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return partition == message.partition &&
                offset == message.offset &&
                timestamp == message.timestamp &&
                crc32 == message.crc32 &&
                Objects.equals(topic, message.topic) &&
                Objects.equals(key, message.key) &&
                Objects.equals(value, message.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, key, value, timestamp, crc32);
    }
}
