package com.kafka.client;

import com.kafka.protocol.*;
import com.kafka.util.Logger;
import com.kafka.util.NetworkUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Kafka-like producer client for sending messages to topics.
 * Handles connection management and message production.
 */
public class KafkaProducer {
    private final String bootstrapServers;
    private final String clientId;
    private Socket socket;
    private boolean connected;

    public KafkaProducer(String bootstrapServers, String clientId) {
        this.bootstrapServers = bootstrapServers;
        this.clientId = clientId;
        this.connected = false;
    }

    /**
     * Connects to the Kafka broker
     */
    public void connect() throws IOException {
        if (connected) {
            return;
        }

        String[] serverParts = bootstrapServers.split(":");
        String host = serverParts[0];
        int port = Integer.parseInt(serverParts[1]);

        socket = new Socket(host, port);
        connected = true;
        
        Logger.info("Producer connected to {}:{}", host, port);
    }

    /**
     * Disconnects from the Kafka broker
     */
    public void disconnect() {
        if (connected) {
            NetworkUtils.closeSocket(socket);
            connected = false;
            Logger.info("Producer disconnected");
        }
    }

    /**
     * Sends a message to a topic
     */
    public long send(String topic, byte[] key, byte[] value) throws IOException {
        return send(topic, 0, key, value); // Default to partition 0
    }

    /**
     * Sends a message to a specific topic partition
     */
    public long send(String topic, int partition, byte[] key, byte[] value) throws IOException {
        if (!connected) {
            connect();
        }

        // Create message (offset will be assigned by server)
        new Message(topic, partition, 0, key, value);
        
        // Create produce request
        Request request = createProduceRequest(topic, partition, key, value);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        if (!response.isSuccess()) {
            throw new IOException("Failed to send message: " + response.getErrorMessage());
        }
        
        // Extract offset from response
        ByteBuffer buffer = ByteBuffer.wrap(response.getData());
        long offset = buffer.getLong();
        
        Logger.debug("Sent message to topic '{}' partition {} at offset {}", 
                   topic, partition, offset);
        
        return offset;
    }

    /**
     * Sends a string message to a topic
     */
    public long send(String topic, String key, String value) throws IOException {
        return send(topic, 0, key, value);
    }

    /**
     * Sends a string message to a specific topic partition
     */
    public long send(String topic, int partition, String key, String value) throws IOException {
        byte[] keyBytes = key != null ? key.getBytes() : null;
        byte[] valueBytes = value.getBytes();
        return send(topic, partition, keyBytes, valueBytes);
    }

    /**
     * Creates a produce request
     */
    private Request createProduceRequest(String topic, int partition, byte[] key, byte[] value) {
        // Serialize request data
        byte[] topicBytes = topic.getBytes();
        int keyLength = key != null ? key.length : 0;
        int valueLength = value.length;
        
        int totalSize = 4 + topicBytes.length + 4 + 4 + keyLength + 4 + valueLength;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write topic name
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        
        // Write partition
        buffer.putInt(partition);
        
        // Write key
        buffer.putInt(keyLength);
        if (key != null) {
            buffer.put(key);
        }
        
        // Write value
        buffer.putInt(valueLength);
        buffer.put(value);
        
        return new Request(RequestType.PRODUCE, buffer.array(), clientId);
    }

    /**
     * Checks if the producer is connected
     */
    public boolean isConnected() {
        return connected && NetworkUtils.isSocketConnected(socket);
    }

    /**
     * Gets the client ID
     */
    public String getClientId() {
        return clientId;
    }

    @Override
    public String toString() {
        return String.format("KafkaProducer{clientId='%s', connected=%s}", clientId, connected);
    }
}
