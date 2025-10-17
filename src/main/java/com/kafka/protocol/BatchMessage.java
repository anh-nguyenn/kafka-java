package com.kafka.protocol;

import com.kafka.util.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a batch of messages for improved throughput.
 * Groups multiple messages together for efficient network transmission.
 */
public class BatchMessage {
    private final String topic;
    private final int partition;
    private final List<Message> messages;
    private final long timestamp;
    private final String batchId;
    private int crc32;

    public BatchMessage(String topic, int partition, List<Message> messages) {
        this.topic = Objects.requireNonNull(topic, "Topic cannot be null");
        this.partition = partition;
        this.messages = new ArrayList<>(Objects.requireNonNull(messages, "Messages cannot be null"));
        this.timestamp = Instant.now().toEpochMilli();
        this.batchId = generateBatchId();
        this.crc32 = calculateCRC32();
    }

    /**
     * Creates a batch message from serialized bytes
     */
    public static BatchMessage deserialize(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        // Read topic name
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        
        // Read partition
        int partition = buffer.getInt();
        
        // Read timestamp
        long timestamp = buffer.getLong();
        
        // Read batch ID
        int batchIdLength = buffer.getInt();
        byte[] batchIdBytes = new byte[batchIdLength];
        buffer.get(batchIdBytes);
        String batchId = new String(batchIdBytes);
        
        // Read CRC32
        int crc32 = buffer.getInt();
        
        // Read message count
        int messageCount = buffer.getInt();
        List<Message> messages = new ArrayList<>();
        
        for (int i = 0; i < messageCount; i++) {
            int messageLength = buffer.getInt();
            byte[] messageData = new byte[messageLength];
            buffer.get(messageData);
            
            Message message = Message.deserialize(messageData);
            messages.add(message);
        }
        
        BatchMessage batchMessage = new BatchMessage(topic, partition, messages);
        batchMessage.timestamp = timestamp;
        batchMessage.batchId = batchId;
        batchMessage.crc32 = crc32;
        
        return batchMessage;
    }

    /**
     * Serializes the batch message to bytes
     */
    public byte[] serialize() throws IOException {
        // Calculate total size
        int topicLength = topic.getBytes().length;
        int batchIdLength = batchId.getBytes().length;
        
        int totalSize = 4 + topicLength + 4 + 8 + 4 + batchIdLength + 4 + 4; // Header
        for (Message message : messages) {
            byte[] messageData = message.serialize();
            totalSize += 4 + messageData.length; // Length + data
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write header
        buffer.putInt(topicLength);
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(timestamp);
        buffer.putInt(batchIdLength);
        buffer.put(batchId.getBytes());
        buffer.putInt(crc32);
        
        // Write message count
        buffer.putInt(messages.size());
        
        // Write messages
        for (Message message : messages) {
            byte[] messageData = message.serialize();
            buffer.putInt(messageData.length);
            buffer.put(messageData);
        }
        
        return buffer.array();
    }

    /**
     * Generates a unique batch ID
     */
    private String generateBatchId() {
        return String.format("batch_%d_%d", timestamp, System.nanoTime() % 1000000);
    }

    /**
     * Calculates CRC32 checksum for data integrity
     */
    private int calculateCRC32() {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(topic.getBytes());
        crc.update(partition);
        crc.update((int) timestamp);
        crc.update(batchId.getBytes());
        
        for (Message message : messages) {
            crc.update(message.serialize());
        }
        
        return (int) crc.getValue();
    }

    /**
     * Validates batch integrity using CRC32
     */
    public boolean isValid() {
        return crc32 == calculateCRC32();
    }

    /**
     * Gets the total size of all messages in the batch
     */
    public int getTotalSize() {
        return messages.stream()
                .mapToInt(msg -> msg.getValue().length)
                .sum();
    }

    /**
     * Gets the average message size in the batch
     */
    public double getAverageMessageSize() {
        if (messages.isEmpty()) {
            return 0.0;
        }
        return (double) getTotalSize() / messages.size();
    }

    /**
     * Gets compression ratio if messages are compressed
     */
    public double getCompressionRatio() {
        long originalSize = messages.stream()
                .mapToLong(msg -> {
                    try {
                        return msg.getDecompressedValue().length;
                    } catch (IOException e) {
                        return msg.getValue().length;
                    }
                })
                .sum();
        
        long compressedSize = getTotalSize();
        
        if (originalSize == 0) {
            return 1.0;
        }
        
        return (double) compressedSize / originalSize;
    }

    // Getters
    public String getTopic() { return topic; }
    public int getPartition() { return partition; }
    public List<Message> getMessages() { return new ArrayList<>(messages); }
    public long getTimestamp() { return timestamp; }
    public String getBatchId() { return batchId; }
    public int getCrc32() { return crc32; }
    public int getMessageCount() { return messages.size(); }

    @Override
    public String toString() {
        return String.format("BatchMessage{topic='%s', partition=%d, messageCount=%d, batchId='%s', timestamp=%d}",
                topic, partition, messages.size(), batchId, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchMessage that = (BatchMessage) o;
        return partition == that.partition &&
                timestamp == that.timestamp &&
                crc32 == that.crc32 &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(messages, that.messages) &&
                Objects.equals(batchId, that.batchId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, messages, timestamp, batchId, crc32);
    }
}
