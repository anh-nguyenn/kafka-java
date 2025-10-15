package com.kafka.client;

import com.kafka.protocol.*;
import com.kafka.util.Logger;
import com.kafka.util.NetworkUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Admin client for managing topics and getting cluster metadata.
 * Provides administrative operations for the Kafka-like broker.
 */
public class AdminClient {
    private final String bootstrapServers;
    private final String clientId;
    private Socket socket;
    private boolean connected;

    public AdminClient(String bootstrapServers, String clientId) {
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
        
        Logger.info("Admin client connected to {}:{}", host, port);
    }

    /**
     * Disconnects from the Kafka broker
     */
    public void disconnect() {
        if (connected) {
            NetworkUtils.closeSocket(socket);
            connected = false;
            Logger.info("Admin client disconnected");
        }
    }

    /**
     * Creates a new topic
     */
    public void createTopic(String topicName, int partitionCount) throws IOException {
        if (!connected) {
            connect();
        }

        // Create create topic request
        Request request = createCreateTopicRequest(topicName, partitionCount);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        if (!response.isSuccess()) {
            throw new IOException("Failed to create topic: " + response.getErrorMessage());
        }
        
        Logger.info("Created topic '{}' with {} partitions", topicName, partitionCount);
    }

    /**
     * Creates a new topic with default partition count
     */
    public void createTopic(String topicName) throws IOException {
        createTopic(topicName, 3); // Default 3 partitions
    }

    /**
     * Lists all topics
     */
    public List<String> listTopics() throws IOException {
        if (!connected) {
            connect();
        }

        // Create list topics request
        Request request = new Request(RequestType.LIST_TOPICS, new byte[0], clientId);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        if (!response.isSuccess()) {
            throw new IOException("Failed to list topics: " + response.getErrorMessage());
        }
        
        // Deserialize topic names
        List<String> topics = deserializeTopicList(response.getData());
        
        Logger.debug("Listed {} topics", topics.size());
        
        return topics;
    }

    /**
     * Gets topic metadata
     */
    public String describeTopic(String topicName) throws IOException {
        if (!connected) {
            connect();
        }

        // Create describe topic request
        Request request = createDescribeTopicRequest(topicName);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        if (!response.isSuccess()) {
            throw new IOException("Failed to describe topic: " + response.getErrorMessage());
        }
        
        String metadata = new String(response.getData());
        
        Logger.debug("Described topic '{}'", topicName);
        
        return metadata;
    }

    /**
     * Pings the broker
     */
    public boolean ping() throws IOException {
        if (!connected) {
            connect();
        }

        // Create ping request
        Request request = new Request(RequestType.PING, new byte[0], clientId);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        boolean success = response.isSuccess();
        
        Logger.debug("Ping result: {}", success ? "success" : "failed");
        
        return success;
    }

    /**
     * Creates a create topic request
     */
    private Request createCreateTopicRequest(String topicName, int partitionCount) {
        byte[] topicBytes = topicName.getBytes();
        
        int totalSize = 4 + topicBytes.length + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write topic name
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        
        // Write partition count
        buffer.putInt(partitionCount);
        
        return new Request(RequestType.CREATE_TOPIC, buffer.array(), clientId);
    }

    /**
     * Creates a describe topic request
     */
    private Request createDescribeTopicRequest(String topicName) {
        byte[] topicBytes = topicName.getBytes();
        
        int totalSize = 4 + topicBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write topic name
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        
        return new Request(RequestType.DESCRIBE_TOPIC, buffer.array(), clientId);
    }

    /**
     * Deserializes topic list from response data
     */
    private List<String> deserializeTopicList(byte[] data) {
        List<String> topics = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int topicCount = buffer.getInt();
        for (int i = 0; i < topicCount; i++) {
            int topicLength = buffer.getInt();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            
            String topicName = new String(topicBytes);
            topics.add(topicName);
        }
        
        return topics;
    }

    /**
     * Checks if the admin client is connected
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
        return String.format("AdminClient{clientId='%s', connected=%s}", clientId, connected);
    }
}
