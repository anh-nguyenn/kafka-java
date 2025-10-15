package com.kafka.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the OffsetManager class
 */
public class OffsetManagerTest {

    private OffsetManager offsetManager;

    @BeforeEach
    public void setUp() {
        offsetManager = new OffsetManager();
    }

    @Test
    public void testCommitOffset() {
        String groupId = "test-group";
        String topicName = "test-topic";
        int partition = 0;
        long offset = 123L;

        offsetManager.commitOffset(groupId, topicName, partition, offset);

        long committedOffset = offsetManager.getCommittedOffset(groupId, topicName, partition);
        assertEquals(offset, committedOffset);
    }

    @Test
    public void testGetCommittedOffsetForNonExistentGroup() {
        String groupId = "non-existent-group";
        String topicName = "test-topic";
        int partition = 0;

        long offset = offsetManager.getCommittedOffset(groupId, topicName, partition);
        assertEquals(-1, offset);
    }

    @Test
    public void testGetCommittedOffsetForNonExistentTopic() {
        String groupId = "test-group";
        String topicName = "non-existent-topic";
        int partition = 0;

        offsetManager.commitOffset(groupId, "test-topic", partition, 123L);
        long offset = offsetManager.getCommittedOffset(groupId, topicName, partition);
        assertEquals(-1, offset);
    }

    @Test
    public void testRegisterConsumer() {
        String groupId = "test-group";
        String consumerId = "consumer-1";

        offsetManager.registerConsumer(groupId, consumerId);

        var consumers = offsetManager.getConsumersInGroup(groupId);
        assertEquals(1, consumers.size());
        assertTrue(consumers.contains(consumerId));
    }

    @Test
    public void testUnregisterConsumer() {
        String groupId = "test-group";
        String consumerId = "consumer-1";

        offsetManager.registerConsumer(groupId, consumerId);
        offsetManager.unregisterConsumer(groupId, consumerId);

        var consumers = offsetManager.getConsumersInGroup(groupId);
        assertEquals(0, consumers.size());
    }

    @Test
    public void testUnregisterConsumerFromEmptyGroup() {
        String groupId = "test-group";
        String consumerId = "consumer-1";

        // Should not throw exception
        offsetManager.unregisterConsumer(groupId, consumerId);
    }

    @Test
    public void testGetConsumerGroups() {
        offsetManager.registerConsumer("group1", "consumer1");
        offsetManager.registerConsumer("group2", "consumer2");

        var groups = offsetManager.getConsumerGroups();
        assertEquals(2, groups.size());
        assertTrue(groups.contains("group1"));
        assertTrue(groups.contains("group2"));
    }

    @Test
    public void testGetAllOffsets() {
        String groupId = "test-group";
        String topicName = "test-topic";

        offsetManager.commitOffset(groupId, topicName, 0, 100L);
        offsetManager.commitOffset(groupId, topicName, 1, 200L);

        var allOffsets = offsetManager.getAllOffsets(groupId);
        assertEquals(1, allOffsets.size());
        assertTrue(allOffsets.containsKey(topicName));

        var topicOffsets = allOffsets.get(topicName);
        assertEquals(2, topicOffsets.size());
        assertEquals(100L, topicOffsets.get(0));
        assertEquals(200L, topicOffsets.get(1));
    }

    @Test
    public void testRemoveGroup() {
        String groupId = "test-group";
        String consumerId = "consumer-1";
        String topicName = "test-topic";

        offsetManager.registerConsumer(groupId, consumerId);
        offsetManager.commitOffset(groupId, topicName, 0, 123L);

        offsetManager.removeGroup(groupId);

        assertEquals(0, offsetManager.getGroupCount());
        assertEquals(-1, offsetManager.getCommittedOffset(groupId, topicName, 0));
    }

    @Test
    public void testGetGroupCount() {
        assertEquals(0, offsetManager.getGroupCount());

        offsetManager.registerConsumer("group1", "consumer1");
        assertEquals(1, offsetManager.getGroupCount());

        offsetManager.registerConsumer("group2", "consumer2");
        assertEquals(2, offsetManager.getGroupCount());
    }
}
