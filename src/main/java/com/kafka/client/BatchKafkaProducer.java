package com.kafka.client;

import com.kafka.config.KafkaConfig;
import com.kafka.protocol.*;
import com.kafka.util.Logger;
import com.kafka.util.NetworkUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Batch Kafka producer for improved throughput.
 * Collects messages and sends them in batches for better performance.
 */
public class BatchKafkaProducer {
    private final String bootstrapServers;
    private final String clientId;
    private final int batchSize;
    private final long batchTimeoutMs;
    private final boolean enableCompression;
    private final int compressionThreshold;

    private Socket socket;
    private boolean connected;
    private final List<Message> pendingMessages;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running;

    public BatchKafkaProducer(String bootstrapServers, String clientId) {
        this.bootstrapServers = bootstrapServers;
        this.clientId = clientId;
        this.batchSize = KafkaConfig.getInt(KafkaConfig.BATCH_SIZE, 1000);
        this.batchTimeoutMs = KafkaConfig.getLong(KafkaConfig.BATCH_TIMEOUT_MS, 1000);
        this.enableCompression = KafkaConfig.getBoolean(KafkaConfig.ENABLE_COMPRESSION, true);
        this.compressionThreshold = KafkaConfig.getInt(KafkaConfig.COMPRESSION_THRESHOLD, 1024);

        this.connected = false;
        this.pendingMessages = new ArrayList<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.running = new AtomicBoolean(false);
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
        running.set(true);

        // Start batch processing
        startBatchProcessor();

        Logger.info("Batch producer connected to {}:{}", host, port);
    }

    /**
     * Disconnects from the Kafka broker
     */
    public void disconnect() {
        if (connected) {
            running.set(false);

            // Send any remaining messages
            try {
                flush();
            } catch (IOException e) {
                Logger.warn("Error flushing messages during disconnect", e);
            }

            NetworkUtils.closeSocket(socket);
            connected = false;
            scheduler.shutdown();

            Logger.info("Batch producer disconnected");
        }
    }

    /**
     * Sends a message (will be batched)
     */
    public void send(String topic, byte[] key, byte[] value) throws IOException {
        send(topic, 0, key, value);
    }

    /**
     * Sends a message to a specific partition (will be batched)
     */
    public void send(String topic, int partition, byte[] key, byte[] value) throws IOException {
        if (!connected) {
            connect();
        }

        // Create message
        Message message = new Message(topic, partition, 0, key, value);

        // Apply compression if enabled and message is large enough
        if (enableCompression && value.length >= compressionThreshold) {
            try {
                message.compressValue();
            } catch (IOException e) {
                Logger.warn("Failed to compress message, sending uncompressed", e);
            }
        }

        synchronized (pendingMessages) {
            pendingMessages.add(message);

            // Send batch if it's full
            if (pendingMessages.size() >= batchSize) {
                sendBatch();
            }
        }
    }

    /**
     * Sends a string message (will be batched)
     */
    public void send(String topic, String key, String value) throws IOException {
        send(topic, 0, key, value);
    }

    /**
     * Sends a string message to a specific partition (will be batched)
     */
    public void send(String topic, int partition, String key, String value) throws IOException {
        byte[] keyBytes = key != null ? key.getBytes() : null;
        byte[] valueBytes = value.getBytes();
        send(topic, partition, keyBytes, valueBytes);
    }

    /**
     * Forces sending of all pending messages
     */
    public void flush() throws IOException {
        synchronized (pendingMessages) {
            if (!pendingMessages.isEmpty()) {
                sendBatch();
            }
        }
    }

    /**
     * Starts the batch processor
     */
    private void startBatchProcessor() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                synchronized (pendingMessages) {
                    if (!pendingMessages.isEmpty()) {
                        sendBatch();
                    }
                }
            } catch (IOException e) {
                Logger.error("Error in batch processor", e);
            }
        }, batchTimeoutMs, batchTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Sends a batch of messages
     */
    private void sendBatch() throws IOException {
        if (pendingMessages.isEmpty()) {
            return;
        }

        List<Message> batchMessages = new ArrayList<>(pendingMessages);
        pendingMessages.clear();

        // Group messages by topic and partition
        var groupedMessages = groupMessagesByTopicAndPartition(batchMessages);

        for (var entry : groupedMessages.entrySet()) {
            String topicPartition = entry.getKey();
            List<Message> messages = entry.getValue();

            String[] parts = topicPartition.split(":");
            String topic = parts[0];
            int partition = Integer.parseInt(parts[1]);

            // Create batch message
            BatchMessage batchMessage = new BatchMessage(topic, partition, messages);

            // Send batch
            sendBatchMessage(batchMessage);
        }
    }

    /**
     * Groups messages by topic and partition
     */
    private java.util.Map<String, List<Message>> groupMessagesByTopicAndPartition(List<Message> messages) {
        java.util.Map<String, List<Message>> grouped = new java.util.HashMap<>();

        for (Message message : messages) {
            String key = message.getTopic() + ":" + message.getPartition();
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(message);
        }

        return grouped;
    }

    /**
     * Sends a batch message to the server
     */
    private void sendBatchMessage(BatchMessage batchMessage) throws IOException {
        // Create batch produce request
        Request request = createBatchProduceRequest(batchMessage);

        // Send request and get response
        byte[] responseData = NetworkUtils.sendRequest(socket, request.serialize());
        Response response = Response.deserialize(responseData);

        if (!response.isSuccess()) {
            throw new IOException("Failed to send batch: " + response.getErrorMessage());
        }

        Logger.debug("Sent batch of {} messages to topic '{}' partition {}",
                batchMessage.getMessageCount(), batchMessage.getTopic(), batchMessage.getPartition());
    }

    /**
     * Creates a batch produce request
     */
    private Request createBatchProduceRequest(BatchMessage batchMessage) throws IOException {
        byte[] batchData = batchMessage.serialize();

        int totalSize = 4 + batchData.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Write batch data length
        buffer.putInt(batchData.length);
        buffer.put(batchData);

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

    /**
     * Gets the number of pending messages
     */
    public int getPendingMessageCount() {
        synchronized (pendingMessages) {
            return pendingMessages.size();
        }
    }

    /**
     * Gets batch statistics
     */
    public String getBatchStatistics() {
        synchronized (pendingMessages) {
            return String.format("BatchProducer{pending=%d, batchSize=%d, timeout=%dms, compression=%s}",
                    pendingMessages.size(), batchSize, batchTimeoutMs, enableCompression);
        }
    }

    @Override
    public String toString() {
        return String.format("BatchKafkaProducer{clientId='%s', connected=%s, pending=%d}",
                clientId, connected, getPendingMessageCount());
    }
}

