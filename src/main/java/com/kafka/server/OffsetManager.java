package com.kafka.server;

import com.kafka.util.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer group offsets for the Kafka-like broker.
 * Tracks the last committed offset for each consumer group and topic partition.
 */
public class OffsetManager {
    private final Map<String, Map<String, Map<Integer, Long>>> offsets; // groupId -> topicName -> partition -> offset
    private final Map<String, Set<String>> consumerGroups; // groupId -> set of consumerIds

    public OffsetManager() {
        this.offsets = new ConcurrentHashMap<>();
        this.consumerGroups = new ConcurrentHashMap<>();
        Logger.info("Initialized OffsetManager");
    }

    /**
     * Commits an offset for a consumer group
     */
    public void commitOffset(String groupId, String topicName, int partition, long offset) {
        offsets.computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
               .computeIfAbsent(topicName, k -> new ConcurrentHashMap<>())
               .put(partition, offset);
        
        Logger.debug("Committed offset {} for group '{}' topic '{}' partition {}", 
                    offset, groupId, topicName, partition);
    }

    /**
     * Gets the committed offset for a consumer group
     */
    public long getCommittedOffset(String groupId, String topicName, int partition) {
        Map<String, Map<Integer, Long>> topicOffsets = offsets.get(groupId);
        if (topicOffsets == null) {
            return -1; // No offsets committed for this group
        }
        
        Map<Integer, Long> partitionOffsets = topicOffsets.get(topicName);
        if (partitionOffsets == null) {
            return -1; // No offsets committed for this topic
        }
        
        return partitionOffsets.getOrDefault(partition, -1L);
    }

    /**
     * Registers a consumer with a consumer group
     */
    public void registerConsumer(String groupId, String consumerId) {
        consumerGroups.computeIfAbsent(groupId, k -> ConcurrentHashMap.newKeySet())
                     .add(consumerId);
        
        Logger.debug("Registered consumer '{}' with group '{}'", consumerId, groupId);
    }

    /**
     * Unregisters a consumer from a consumer group
     */
    public void unregisterConsumer(String groupId, String consumerId) {
        Set<String> consumers = consumerGroups.get(groupId);
        if (consumers != null) {
            consumers.remove(consumerId);
            if (consumers.isEmpty()) {
                consumerGroups.remove(groupId);
            }
        }
        
        Logger.debug("Unregistered consumer '{}' from group '{}'", consumerId, groupId);
    }

    /**
     * Gets all consumer groups
     */
    public Set<String> getConsumerGroups() {
        return new HashSet<>(consumerGroups.keySet());
    }

    /**
     * Gets consumers in a group
     */
    public Set<String> getConsumersInGroup(String groupId) {
        Set<String> consumers = consumerGroups.get(groupId);
        return consumers != null ? new HashSet<>(consumers) : Collections.emptySet();
    }

    /**
     * Gets all committed offsets for a consumer group
     */
    public Map<String, Map<Integer, Long>> getAllOffsets(String groupId) {
        Map<String, Map<Integer, Long>> groupOffsets = offsets.get(groupId);
        if (groupOffsets == null) {
            return Collections.emptyMap();
        }
        
        Map<String, Map<Integer, Long>> result = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Long>> entry : groupOffsets.entrySet()) {
            result.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }
        
        return result;
    }

    /**
     * Removes all offsets for a consumer group
     */
    public void removeGroup(String groupId) {
        offsets.remove(groupId);
        consumerGroups.remove(groupId);
        Logger.info("Removed consumer group '{}' and all its offsets", groupId);
    }

    /**
     * Gets the number of consumer groups
     */
    public int getGroupCount() {
        return consumerGroups.size();
    }
}
