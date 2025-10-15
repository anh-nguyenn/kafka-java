package com.kafka.examples;

import com.kafka.client.AdminClient;
import com.kafka.client.KafkaConsumer;
import com.kafka.client.KafkaProducer;
import com.kafka.protocol.Message;

import java.io.IOException;
import java.util.List;

/**
 * Simple example demonstrating how to use the Kafka-like broker
 */
public class SimpleExample {
    
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        
        try {
            // Create admin client
            AdminClient adminClient = new AdminClient(bootstrapServers, "example-admin");
            adminClient.connect();
            
            // Create producer
            KafkaProducer producer = new KafkaProducer(bootstrapServers, "example-producer");
            producer.connect();
            
            // Create consumer
            KafkaConsumer consumer = new KafkaConsumer(bootstrapServers, "example-consumer", "example-group");
            consumer.connect();
            
            // Create a topic
            System.out.println("Creating topic 'example-topic'...");
            adminClient.createTopic("example-topic", 3);
            
            // List topics
            System.out.println("Available topics: " + adminClient.listTopics());
            
            // Produce some messages
            System.out.println("Producing messages...");
            producer.send("example-topic", "key1", "Hello, Kafka!");
            producer.send("example-topic", "key2", "This is a test message");
            producer.send("example-topic", 1, "key3", "Message to partition 1");
            
            // Consume messages
            System.out.println("Consuming messages from partition 0...");
            List<Message> messages = consumer.consume("example-topic", 0, 0, 10);
            for (Message message : messages) {
                System.out.println("Received: " + new String(message.getValue()));
            }
            
            // Consume messages from partition 1
            System.out.println("Consuming messages from partition 1...");
            List<Message> messagesPartition1 = consumer.consume("example-topic", 1, 0, 10);
            for (Message message : messagesPartition1) {
                System.out.println("Received: " + new String(message.getValue()));
            }
            
            // Commit offset
            System.out.println("Committing offset...");
            consumer.commitOffset("example-topic", 0, messages.size() - 1);
            
            // Get topic metadata
            System.out.println("Topic metadata: " + adminClient.describeTopic("example-topic"));
            
            // Clean up
            consumer.disconnect();
            producer.disconnect();
            adminClient.disconnect();
            
            System.out.println("Example completed successfully!");
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
