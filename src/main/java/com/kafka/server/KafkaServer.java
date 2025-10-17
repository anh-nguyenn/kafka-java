package com.kafka.server;

import com.kafka.config.KafkaConfig;
import com.kafka.protocol.*;
import com.kafka.util.Logger;
import com.kafka.util.NetworkUtils;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main Kafka-like broker server implementation.
 * Handles client connections, request processing, and message operations.
 */
public class KafkaServer {
    private final int port;
    private final TopicManager topicManager;
    private final OffsetManager offsetManager;
    private final ExecutorService threadPool;
    private final AtomicBoolean running;
    private ServerSocket serverSocket;

    public KafkaServer(int port) {
        this.port = port;
        int defaultPartitions = KafkaConfig.getInt(KafkaConfig.DEFAULT_PARTITIONS, 3);
        int threadPoolSize = KafkaConfig.getInt(KafkaConfig.THREAD_POOL_SIZE, 10);
        
        this.topicManager = new TopicManager(defaultPartitions);
        this.offsetManager = new OffsetManager();
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
        this.running = new AtomicBoolean(false);
        
        Logger.info("Kafka server initialized with {} threads, {} default partitions", 
                   threadPoolSize, defaultPartitions);
    }

    /**
     * Starts the Kafka server
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);
        
        Logger.info("Kafka server started on port {}", port);
        
        // Start accepting connections in a separate thread
        threadPool.submit(this::acceptConnections);
    }

    /**
     * Stops the Kafka server
     */
    public void stop() {
        running.set(false);
        
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            Logger.error("Error closing server socket", e);
        }
        
        threadPool.shutdown();
        Logger.info("Kafka server stopped");
    }

    /**
     * Accepts client connections and processes them
     */
    private void acceptConnections() {
        while (running.get()) {
            try {
                Socket clientSocket = serverSocket.accept();
                Logger.info("New client connected: {}", clientSocket.getRemoteSocketAddress());
                
                // Handle each client in a separate thread
                threadPool.submit(() -> handleClient(clientSocket));
                
            } catch (IOException e) {
                if (running.get()) {
                    Logger.error("Error accepting client connection", e);
                }
            }
        }
    }

    /**
     * Handles a client connection
     */
    private void handleClient(Socket clientSocket) {
        try (Socket socket = clientSocket) {
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            
            while (running.get() && NetworkUtils.isSocketConnected(socket)) {
                try {
                    // Read request
                    byte[] requestData = NetworkUtils.readMessage(inputStream);
                    Request request = Request.deserialize(requestData);
                    
                    Logger.debug("Received request: {}", request);
                    
                    // Process request
                    Response response = processRequest(request);
                    
                    // Send response
                    byte[] responseData = response.serialize();
                    NetworkUtils.writeMessage(outputStream, responseData);
                    
                    Logger.debug("Sent response: {}", response);
                    
                } catch (IOException e) {
                    Logger.warn("Client disconnected: {}", e.getMessage());
                    break;
                } catch (Exception e) {
                    Logger.error("Error processing request", e);
                    
                    // Send error response
                    Response errorResponse = new Response(Response.Status.INTERNAL_ERROR, 
                                                       "Internal server error: " + e.getMessage());
                    try {
                        byte[] errorData = errorResponse.serialize();
                        NetworkUtils.writeMessage(outputStream, errorData);
                    } catch (IOException ioException) {
                        Logger.error("Error sending error response", ioException);
                    }
                }
            }
        } catch (IOException e) {
            Logger.error("Error handling client", e);
        } finally {
            Logger.info("Client disconnected: {}", clientSocket.getRemoteSocketAddress());
        }
    }

    /**
     * Processes a client request and returns a response
     */
    private Response processRequest(Request request) {
        try {
            switch (request.getType()) {
                case PRODUCE:
                    return handleProduceRequest(request);
                case PRODUCE_BATCH:
                    return handleBatchProduceRequest(request);
                case FETCH:
                    return handleFetchRequest(request);
                case CONSUME:
                    return handleConsumeRequest(request);
                case CREATE_TOPIC:
                    return handleCreateTopicRequest(request);
                case LIST_TOPICS:
                    return handleListTopicsRequest(request);
                case DESCRIBE_TOPIC:
                    return handleDescribeTopicRequest(request);
                case COMMIT_OFFSET:
                    return handleCommitOffsetRequest(request);
                case GET_OFFSET:
                    return handleGetOffsetRequest(request);
                case PING:
                    return handlePingRequest(request);
                default:
                    return new Response(Response.Status.BAD_REQUEST, "Unknown request type");
            }
        } catch (Exception e) {
            Logger.error("Error processing request", e);
            return new Response(Response.Status.INTERNAL_ERROR, e.getMessage());
        }
    }

    private Response handleProduceRequest(Request request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        
        // Read topic name
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);
        
        // Read partition
        int partition = buffer.getInt();
        
        // Read key
        int keyLength = buffer.getInt();
        byte[] key = null;
        if (keyLength > 0) {
            key = new byte[keyLength];
            buffer.get(key);
        }
        
        // Read value
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        
        // Create message
        Message message = new Message(topicName, partition, 0, key, value);
        
        // Produce message
        long offset = topicManager.produceMessage(topicName, message);
        
        // Create response
        ByteBuffer responseBuffer = ByteBuffer.allocate(8);
        responseBuffer.putLong(offset);
        
        return new Response(Response.Status.SUCCESS, responseBuffer.array());
    }

    private Response handleBatchProduceRequest(Request request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        
        // Read batch data length
        int batchDataLength = buffer.getInt();
        byte[] batchData = new byte[batchDataLength];
        buffer.get(batchData);
        
        try {
            // Deserialize batch message
            BatchMessage batchMessage = BatchMessage.deserialize(batchData);
            
            // Validate batch
            if (!batchMessage.isValid()) {
                return new Response(Response.Status.BAD_REQUEST, "Invalid batch message");
            }
            
            // Process each message in the batch
            long totalOffset = 0;
            for (Message message : batchMessage.getMessages()) {
                long offset = topicManager.produceMessage(batchMessage.getTopic(), message);
                totalOffset += offset;
            }
            
            // Create response with batch statistics
            ByteBuffer responseBuffer = ByteBuffer.allocate(16);
            responseBuffer.putLong(totalOffset);
            responseBuffer.putInt(batchMessage.getMessageCount());
            
            Logger.info("Processed batch of {} messages for topic '{}' partition {}", 
                       batchMessage.getMessageCount(), batchMessage.getTopic(), batchMessage.getPartition());
            
            return new Response(Response.Status.SUCCESS, responseBuffer.array());
            
        } catch (IOException e) {
            Logger.error("Error processing batch message", e);
            return new Response(Response.Status.INTERNAL_ERROR, "Failed to process batch: " + e.getMessage());
        }
    }

    private Response handleFetchRequest(Request request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        
        // Read topic name
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);
        
        // Read partition
        int partition = buffer.getInt();
        
        // Read offset
        long offset = buffer.getLong();
        
        // Read max messages
        int maxMessages = buffer.getInt();
        
        // Fetch messages
        var messages = topicManager.fetchMessages(topicName, partition, offset, maxMessages);
        
        // Serialize messages
        ByteBuffer responseBuffer = ByteBuffer.allocate(4 + messages.size() * 1024); // Rough estimate
        responseBuffer.putInt(messages.size());
        
        for (Message message : messages) {
            byte[] messageData = message.serialize();
            responseBuffer.putInt(messageData.length);
            responseBuffer.put(messageData);
        }
        
        byte[] responseData = new byte[responseBuffer.position()];
        responseBuffer.rewind();
        responseBuffer.get(responseData);
        
        return new Response(Response.Status.SUCCESS, responseData);
    }

    private Response handleConsumeRequest(Request request) {
        // Similar to fetch but with offset management
        return handleFetchRequest(request);
    }

    private Response handleCreateTopicRequest(Request request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        
        // Read topic name
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);
        
        // Read partition count
        int partitionCount = buffer.getInt();
        
        // Create topic
        topicManager.createTopic(topicName, partitionCount);
        
        return new Response(Response.Status.SUCCESS, new byte[0]);
    }

    private Response handleListTopicsRequest(Request request) {
        var topics = topicManager.listTopics();
        
        ByteBuffer buffer = ByteBuffer.allocate(topics.size() * 64); // Rough estimate
        buffer.putInt(topics.size());
        
        for (String topicName : topics) {
            byte[] nameBytes = topicName.getBytes();
            buffer.putInt(nameBytes.length);
            buffer.put(nameBytes);
        }
        
        byte[] responseData = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(responseData);
        
        return new Response(Response.Status.SUCCESS, responseData);
    }

    private Response handleDescribeTopicRequest(Request request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        
        // Read topic name
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);
        
        // Get topic metadata
        var metadata = topicManager.getTopicMetadata(topicName);
        if (metadata == null) {
            return new Response(Response.Status.TOPIC_NOT_FOUND, "Topic not found: " + topicName);
        }
        
        // Serialize metadata (simplified)
        String metadataJson = metadata.toString();
        return new Response(Response.Status.SUCCESS, metadataJson.getBytes());
    }

    private Response handleCommitOffsetRequest(Request request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        
        // Read group ID
        int groupIdLength = buffer.getInt();
        byte[] groupIdBytes = new byte[groupIdLength];
        buffer.get(groupIdBytes);
        String groupId = new String(groupIdBytes);
        
        // Read topic name
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);
        
        // Read partition
        int partition = buffer.getInt();
        
        // Read offset
        long offset = buffer.getLong();
        
        // Commit offset
        offsetManager.commitOffset(groupId, topicName, partition, offset);
        
        return new Response(Response.Status.SUCCESS, new byte[0]);
    }

    private Response handleGetOffsetRequest(Request request) {
        ByteBuffer buffer = ByteBuffer.wrap(request.getData());
        
        // Read group ID
        int groupIdLength = buffer.getInt();
        byte[] groupIdBytes = new byte[groupIdLength];
        buffer.get(groupIdBytes);
        String groupId = new String(groupIdBytes);
        
        // Read topic name
        int topicLength = buffer.getInt();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);
        
        // Read partition
        int partition = buffer.getInt();
        
        // Get offset
        long offset = offsetManager.getCommittedOffset(groupId, topicName, partition);
        
        ByteBuffer responseBuffer = ByteBuffer.allocate(8);
        responseBuffer.putLong(offset);
        
        return new Response(Response.Status.SUCCESS, responseBuffer.array());
    }

    private Response handlePingRequest(Request request) {
        return new Response(Response.Status.SUCCESS, "pong".getBytes());
    }

    /**
     * Main method to start the server
     */
    public static void main(String[] args) {
        // Print configuration on startup
        KafkaConfig.printConfiguration();
        
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 
                  KafkaConfig.getInt(KafkaConfig.SERVER_PORT, 9092);
        
        KafkaServer server = new KafkaServer(port);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Logger.info("Shutting down server...");
            server.stop();
        }));
        
        try {
            server.start();
            
            // Keep the main thread alive
            while (server.running.get()) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            Logger.error("Error starting server", e);
            System.exit(1);
        }
    }
}
