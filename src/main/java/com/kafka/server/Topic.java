package com.kafka.server;

import com.kafka.protocol.Message;
import com.kafka.util.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a topic in the Kafka-like broker.
 * Manages partitions and message storage for a topic.
 */
public class Topic {
    private final String name;
    private final int partitionCount;
    private final Map<Integer, List<Message>> partitions;
    private final Map<Integer, AtomicLong> partitionOffsets;
    private final long createdAt;

    public Topic(String name, int partitionCount) {
        this.name = Objects.requireNonNull(name, "Topic name cannot be null");
        this.partitionCount = partitionCount;
        this.partitions = new ConcurrentHashMap<>();
        this.partitionOffsets = new ConcurrentHashMap<>();
        this.createdAt = System.currentTimeMillis();
        
        // Initialize partitions
        for (int i = 0; i < partitionCount; i++) {
            partitions.put(i, Collections.synchronizedList(new ArrayList<>()));
            partitionOffsets.put(i, new AtomicLong(0));
        }
        
        Logger.info("Created topic '{}' with {} partitions", name, partitionCount);
    }

    /**
     * Produces a message to the specified partition
     */
    public long produceMessage(Message message) {
        int partition = message.getPartition();
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }
        
        List<Message> partitionMessages = partitions.get(partition);
        long offset = partitionOffsets.get(partition).getAndIncrement();
        
        // Create a new message with the assigned offset
        Message messageWithOffset = new Message(
            message.getTopic(),
            partition,
            offset,
            message.getKey(),
            message.getValue()
        );
        
        partitionMessages.add(messageWithOffset);
        
        Logger.debug("Produced message to topic '{}' partition {} at offset {}", 
                    name, partition, offset);
        
        return offset;
    }

    /**
     * Fetches messages from a partition starting from the given offset
     */
    public List<Message> fetchMessages(int partition, long offset, int maxMessages) {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }
        
        List<Message> partitionMessages = partitions.get(partition);
        List<Message> result = new ArrayList<>();
        
        synchronized (partitionMessages) {
            for (int i = (int) offset; i < partitionMessages.size() && result.size() < maxMessages; i++) {
                result.add(partitionMessages.get(i));
            }
        }
        
        Logger.debug("Fetched {} messages from topic '{}' partition {} starting at offset {}", 
                    result.size(), name, partition, offset);
        
        return result;
    }

    /**
     * Gets the latest offset for a partition
     */
    public long getLatestOffset(int partition) {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }
        
        return partitionOffsets.get(partition).get() - 1;
    }

    /**
     * Gets the earliest offset for a partition (always 0)
     */
    public long getEarliestOffset(int partition) {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }
        
        return 0;
    }

    /**
     * Gets the number of messages in a partition
     */
    public int getMessageCount(int partition) {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }
        
        return partitions.get(partition).size();
    }

    // Getters
    public String getName() { return name; }
    public int getPartitionCount() { return partitionCount; }
    public long getCreatedAt() { return createdAt; }

    @Override
    public String toString() {
        return String.format("Topic{name='%s', partitions=%d, createdAt=%d}", 
                           name, partitionCount, createdAt);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Topic topic = (Topic) o;
        return Objects.equals(name, topic.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
