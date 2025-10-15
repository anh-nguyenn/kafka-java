package com.kafka.client;

import com.kafka.protocol.*;
import com.kafka.util.Logger;
import com.kafka.util.NetworkUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Kafka-like consumer client for consuming messages from topics.
 * Handles connection management, message consumption, and offset tracking.
 */
public class KafkaConsumer {
    private final String bootstrapServers;
    private final String clientId;
    private final String groupId;
    private Socket socket;
    private boolean connected;

    public KafkaConsumer(String bootstrapServers, String clientId, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.clientId = clientId;
        this.groupId = groupId;
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
        
        Logger.info("Consumer connected to {}:{}", host, port);
    }

    /**
     * Disconnects from the Kafka broker
     */
    public void disconnect() {
        if (connected) {
            NetworkUtils.closeSocket(socket);
            connected = false;
            Logger.info("Consumer disconnected");
        }
    }

    /**
     * Consumes messages from a topic partition
     */
    public List<Message> consume(String topic, int partition, long offset, int maxMessages) throws IOException {
        if (!connected) {
            connect();
        }

        // Create fetch request
        Request request = createFetchRequest(topic, partition, offset, maxMessages);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        if (!response.isSuccess()) {
            throw new IOException("Failed to consume messages: " + response.getErrorMessage());
        }
        
        // Deserialize messages from response
        List<Message> messages = deserializeMessages(response.getData());
        
        Logger.debug("Consumed {} messages from topic '{}' partition {} starting at offset {}", 
                   messages.size(), topic, partition, offset);
        
        return messages;
    }

    /**
     * Consumes messages from a topic (partition 0)
     */
    public List<Message> consume(String topic, long offset, int maxMessages) throws IOException {
        return consume(topic, 0, offset, maxMessages);
    }

    /**
     * Commits an offset for the consumer group
     */
    public void commitOffset(String topic, int partition, long offset) throws IOException {
        if (!connected) {
            connect();
        }

        // Create commit offset request
        Request request = createCommitOffsetRequest(topic, partition, offset);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        if (!response.isSuccess()) {
            throw new IOException("Failed to commit offset: " + response.getErrorMessage());
        }
        
        Logger.debug("Committed offset {} for group '{}' topic '{}' partition {}", 
                   offset, groupId, topic, partition);
    }

    /**
     * Gets the committed offset for the consumer group
     */
    public long getCommittedOffset(String topic, int partition) throws IOException {
        if (!connected) {
            connect();
        }

        // Create get offset request
        Request request = createGetOffsetRequest(topic, partition);
        
        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);
        
        if (!response.isSuccess()) {
            throw new IOException("Failed to get offset: " + response.getErrorMessage());
        }
        
        // Extract offset from response
        ByteBuffer buffer = ByteBuffer.wrap(response.getData());
        long offset = buffer.getLong();
        
        Logger.debug("Got committed offset {} for group '{}' topic '{}' partition {}", 
                   offset, groupId, topic, partition);
        
        return offset;
    }

    /**
     * Creates a fetch request
     */
    private Request createFetchRequest(String topic, int partition, long offset, int maxMessages) {
        byte[] topicBytes = topic.getBytes();
        
        int totalSize = 4 + topicBytes.length + 4 + 8 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write topic name
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        
        // Write partition
        buffer.putInt(partition);
        
        // Write offset
        buffer.putLong(offset);
        
        // Write max messages
        buffer.putInt(maxMessages);
        
        return new Request(RequestType.FETCH, buffer.array(), clientId);
    }

    /**
     * Creates a commit offset request
     */
    private Request createCommitOffsetRequest(String topic, int partition, long offset) {
        byte[] groupIdBytes = groupId.getBytes();
        byte[] topicBytes = topic.getBytes();
        
        int totalSize = 4 + groupIdBytes.length + 4 + topicBytes.length + 4 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write group ID
        buffer.putInt(groupIdBytes.length);
        buffer.put(groupIdBytes);
        
        // Write topic name
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        
        // Write partition
        buffer.putInt(partition);
        
        // Write offset
        buffer.putLong(offset);
        
        return new Request(RequestType.COMMIT_OFFSET, buffer.array(), clientId);
    }

    /**
     * Creates a get offset request
     */
    private Request createGetOffsetRequest(String topic, int partition) {
        byte[] groupIdBytes = groupId.getBytes();
        byte[] topicBytes = topic.getBytes();
        
        int totalSize = 4 + groupIdBytes.length + 4 + topicBytes.length + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write group ID
        buffer.putInt(groupIdBytes.length);
        buffer.put(groupIdBytes);
        
        // Write topic name
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        
        // Write partition
        buffer.putInt(partition);
        
        return new Request(RequestType.GET_OFFSET, buffer.array(), clientId);
    }

    /**
     * Deserializes messages from response data
     */
    private List<Message> deserializeMessages(byte[] data) {
        List<Message> messages = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int messageCount = buffer.getInt();
        for (int i = 0; i < messageCount; i++) {
            int messageLength = buffer.getInt();
            byte[] messageData = new byte[messageLength];
            buffer.get(messageData);
            
            Message message = Message.deserialize(messageData);
            messages.add(message);
        }
        
        return messages;
    }

    /**
     * Checks if the consumer is connected
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

    /**
     * Gets the group ID
     */
    public String getGroupId() {
        return groupId;
    }

    @Override
    public String toString() {
        return String.format("KafkaConsumer{clientId='%s', groupId='%s', connected=%s}", 
                           clientId, groupId, connected);
    }
}
