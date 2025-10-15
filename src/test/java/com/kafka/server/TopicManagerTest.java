package com.kafka.server;

import com.kafka.protocol.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the TopicManager class
 */
public class TopicManagerTest {

    private TopicManager topicManager;

    @BeforeEach
    public void setUp() {
        topicManager = new TopicManager(3);
    }

    @Test
    public void testCreateTopic() {
        Topic topic = topicManager.createTopic("test-topic", 5);

        assertNotNull(topic);
        assertEquals("test-topic", topic.getName());
        assertEquals(5, topic.getPartitionCount());
        assertTrue(topicManager.topicExists("test-topic"));
    }

    @Test
    public void testCreateTopicWithDefaultPartitions() {
        Topic topic = topicManager.createTopic("test-topic");

        assertNotNull(topic);
        assertEquals("test-topic", topic.getName());
        assertEquals(3, topic.getPartitionCount());
    }

    @Test
    public void testCreateDuplicateTopic() {
        topicManager.createTopic("test-topic");

        assertThrows(IllegalArgumentException.class, () -> {
            topicManager.createTopic("test-topic");
        });
    }

    @Test
    public void testGetTopic() {
        Topic createdTopic = topicManager.createTopic("test-topic");
        Topic retrievedTopic = topicManager.getTopic("test-topic");

        assertEquals(createdTopic, retrievedTopic);
    }

    @Test
    public void testGetNonExistentTopic() {
        Topic topic = topicManager.getTopic("non-existent");
        assertNull(topic);
    }

    @Test
    public void testListTopics() {
        topicManager.createTopic("topic1");
        topicManager.createTopic("topic2");

        var topics = topicManager.listTopics();

        assertEquals(2, topics.size());
        assertTrue(topics.contains("topic1"));
        assertTrue(topics.contains("topic2"));
    }

    @Test
    public void testProduceMessage() {
        topicManager.createTopic("test-topic");
        byte[] value = "test-value".getBytes();
        Message message = new Message("test-topic", 0, 0, null, value);

        long offset = topicManager.produceMessage("test-topic", message);

        assertEquals(0, offset);
    }

    @Test
    public void testProduceMessageToNonExistentTopic() {
        byte[] value = "test-value".getBytes();
        Message message = new Message("test-topic", 0, 0, null, value);

        assertThrows(IllegalArgumentException.class, () -> {
            topicManager.produceMessage("test-topic", message);
        });
    }

    @Test
    public void testFetchMessages() {
        topicManager.createTopic("test-topic");
        byte[] value = "test-value".getBytes();
        Message message = new Message("test-topic", 0, 0, null, value);
        topicManager.produceMessage("test-topic", message);

        var messages = topicManager.fetchMessages("test-topic", 0, 0, 10);

        assertEquals(1, messages.size());
        assertEquals("test-value", new String(messages.get(0).getValue()));
    }

    @Test
    public void testGetTopicMetadata() {
        Topic topic = topicManager.createTopic("test-topic", 2);
        var metadata = topicManager.getTopicMetadata("test-topic");

        assertNotNull(metadata);
        assertEquals("test-topic", metadata.get("name"));
        assertEquals(2, metadata.get("partitionCount"));
        assertTrue(metadata.containsKey("partitions"));
    }

    @Test
    public void testGetTopicCount() {
        assertEquals(0, topicManager.getTopicCount());
        
        topicManager.createTopic("topic1");
        assertEquals(1, topicManager.getTopicCount());
        
        topicManager.createTopic("topic2");
        assertEquals(2, topicManager.getTopicCount());
    }
}
