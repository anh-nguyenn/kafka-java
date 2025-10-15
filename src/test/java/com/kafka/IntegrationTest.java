package com.kafka;

import com.kafka.client.AdminClient;
import com.kafka.client.KafkaConsumer;
import com.kafka.client.KafkaProducer;
import com.kafka.protocol.Message;
import com.kafka.server.KafkaServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Kafka-like broker
 */
public class IntegrationTest {

    private KafkaServer server;
    private AdminClient adminClient;
    private KafkaProducer producer;
    private KafkaConsumer consumer;

    @BeforeEach
    public void setUp() throws IOException, InterruptedException {
        // Start server
        server = new KafkaServer(9092);
        server.start();
        
        // Wait for server to start
        TimeUnit.MILLISECONDS.sleep(100);
        
        // Create clients
        adminClient = new AdminClient("localhost:9092", "admin-client");
        producer = new KafkaProducer("localhost:9092", "producer-client");
        consumer = new KafkaConsumer("localhost:9092", "consumer-client", "test-group");
        
        // Connect clients
        adminClient.connect();
        producer.connect();
        consumer.connect();
    }

    @AfterEach
    public void tearDown() {
        if (consumer != null) consumer.disconnect();
        if (producer != null) producer.disconnect();
        if (adminClient != null) adminClient.disconnect();
        if (server != null) server.stop();
    }

    @Test
    public void testEndToEndMessageFlow() throws IOException {
        // Create topic
        adminClient.createTopic("test-topic", 3);
        
        // Verify topic was created
        List<String> topics = adminClient.listTopics();
        assertTrue(topics.contains("test-topic"));
        
        // Produce messages
        long offset1 = producer.send("test-topic", "key1", "value1");
        long offset2 = producer.send("test-topic", "key2", "value2");
        long offset3 = producer.send("test-topic", 1, "key3", "value3"); // Different partition
        
        assertEquals(0, offset1);
        assertEquals(1, offset2);
        assertEquals(0, offset3); // Different partition
        
        // Consume messages from partition 0
        List<Message> messages = consumer.consume("test-topic", 0, 0, 10);
        assertEquals(2, messages.size());
        assertEquals("value1", new String(messages.get(0).getValue()));
        assertEquals("value2", new String(messages.get(1).getValue()));
        
        // Consume messages from partition 1
        List<Message> messagesPartition1 = consumer.consume("test-topic", 1, 0, 10);
        assertEquals(1, messagesPartition1.size());
        assertEquals("value3", new String(messagesPartition1.get(0).getValue()));
    }

    @Test
    public void testOffsetManagement() throws IOException {
        // Create topic
        adminClient.createTopic("test-topic");
        
        // Produce messages
        producer.send("test-topic", "key1", "value1");
        producer.send("test-topic", "key2", "value2");
        
        // Consume messages
        List<Message> messages = consumer.consume("test-topic", 0, 0, 10);
        assertEquals(2, messages.size());
        
        // Commit offset
        consumer.commitOffset("test-topic", 0, 1);
        
        // Verify committed offset
        long committedOffset = consumer.getCommittedOffset("test-topic", 0);
        assertEquals(1, committedOffset);
    }

    @Test
    public void testMultipleConsumers() throws IOException {
        // Create topic
        adminClient.createTopic("test-topic");
        
        // Produce messages
        producer.send("test-topic", "key1", "value1");
        producer.send("test-topic", "key2", "value2");
        producer.send("test-topic", "key3", "value3");
        
        // Create second consumer
        KafkaConsumer consumer2 = new KafkaConsumer("localhost:9092", "consumer-2", "test-group");
        consumer2.connect();
        
        try {
            // Both consumers should be able to consume messages
            List<Message> messages1 = consumer.consume("test-topic", 0, 0, 10);
            List<Message> messages2 = consumer2.consume("test-topic", 0, 0, 10);
            
            assertEquals(3, messages1.size());
            assertEquals(3, messages2.size());
        } finally {
            consumer2.disconnect();
        }
    }

    @Test
    public void testServerPing() throws IOException {
        boolean pingResult = adminClient.ping();
        assertTrue(pingResult);
    }

    @Test
    public void testTopicMetadata() throws IOException {
        // Create topic
        adminClient.createTopic("test-topic", 2);
        
        // Get topic metadata
        String metadata = adminClient.describeTopic("test-topic");
        assertNotNull(metadata);
        assertTrue(metadata.contains("test-topic"));
    }
}
