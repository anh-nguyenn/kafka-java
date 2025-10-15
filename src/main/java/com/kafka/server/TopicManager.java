package com.kafka.server;

import com.kafka.protocol.Message;
import com.kafka.util.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages all topics in the Kafka-like broker.
 * Handles topic creation, retrieval, and message operations.
 */
public class TopicManager {
    private final Map<String, Topic> topics;
    private final int defaultPartitionCount;

    public TopicManager(int defaultPartitionCount) {
        this.topics = new ConcurrentHashMap<>();
        this.defaultPartitionCount = defaultPartitionCount;
        Logger.info("Initialized TopicManager with default partition count: {}", defaultPartitionCount);
    }

    /**
     * Creates a new topic
     */
    public Topic createTopic(String name, int partitionCount) {
        if (topics.containsKey(name)) {
            throw new IllegalArgumentException("Topic already exists: " + name);
        }
        
        Topic topic = new Topic(name, partitionCount);
        topics.put(name, topic);
        
        Logger.info("Created topic '{}' with {} partitions", name, partitionCount);
        return topic;
    }

    /**
     * Creates a new topic with default partition count
     */
    public Topic createTopic(String name) {
        return createTopic(name, defaultPartitionCount);
    }

    /**
     * Gets a topic by name
     */
    public Topic getTopic(String name) {
        return topics.get(name);
    }

    /**
     * Checks if a topic exists
     */
    public boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    /**
     * Lists all topic names
     */
    public Set<String> listTopics() {
        return new HashSet<>(topics.keySet());
    }

    /**
     * Produces a message to a topic
     */
    public long produceMessage(String topicName, Message message) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        
        return topic.produceMessage(message);
    }

    /**
     * Fetches messages from a topic partition
     */
    public List<Message> fetchMessages(String topicName, int partition, long offset, int maxMessages) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        
        return topic.fetchMessages(partition, offset, maxMessages);
    }

    /**
     * Gets the latest offset for a topic partition
     */
    public long getLatestOffset(String topicName, int partition) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        
        return topic.getLatestOffset(partition);
    }

    /**
     * Gets the earliest offset for a topic partition
     */
    public long getEarliestOffset(String topicName, int partition) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        
        return topic.getEarliestOffset(partition);
    }

    /**
     * Gets topic metadata
     */
    public Map<String, Object> getTopicMetadata(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            return null;
        }
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("name", topic.getName());
        metadata.put("partitionCount", topic.getPartitionCount());
        metadata.put("createdAt", topic.getCreatedAt());
        
        Map<String, Object> partitionInfo = new HashMap<>();
        for (int i = 0; i < topic.getPartitionCount(); i++) {
            Map<String, Object> partitionData = new HashMap<>();
            partitionData.put("messageCount", topic.getMessageCount(i));
            partitionData.put("latestOffset", topic.getLatestOffset(i));
            partitionData.put("earliestOffset", topic.getEarliestOffset(i));
            partitionInfo.put(String.valueOf(i), partitionData);
        }
        metadata.put("partitions", partitionInfo);
        
        return metadata;
    }

    /**
     * Gets the number of topics
     */
    public int getTopicCount() {
        return topics.size();
    }
}
