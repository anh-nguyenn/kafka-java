# Comprehensive Documentation: Kafka-like Message Broker in Java

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Overview](#architecture-overview)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [Protocol Design](#protocol-design)
6. [Features Implementation](#features-implementation)
7. [Configuration System](#configuration-system)
8. [Client APIs](#client-apis)
9. [Server Implementation](#server-implementation)
10. [Testing Strategy](#testing-strategy)
11. [Getting Started Guide](#getting-started-guide)
12. [Advanced Usage](#advanced-usage)
13. [Performance Considerations](#performance-considerations)
14. [Troubleshooting](#troubleshooting)
15. [API Reference](#api-reference)

---

## Project Overview

This project is a **simplified implementation of Apache Kafka** built entirely in Java 17. It demonstrates the core concepts of distributed messaging systems including:

- **Message Production**: Sending messages to topics
- **Message Consumption**: Reading messages from topics
- **Topic Management**: Creating and managing topics with partitions
- **Offset Management**: Tracking consumer progress
- **Binary Protocol**: Efficient message serialization
- **Concurrent Processing**: Multi-threaded server architecture
- **Monitoring**: Metrics collection and health checking

### Key Features

- ✅ **Topic Management**: Create topics with configurable partitions
- ✅ **Message Production**: Send messages with keys and values
- ✅ **Message Consumption**: Consume messages with offset tracking
- ✅ **Partitioning**: Distribute messages across multiple partitions
- ✅ **Offset Management**: Track consumer group progress
- ✅ **Binary Protocol**: Efficient message serialization/deserialization
- ✅ **Concurrent Processing**: Multi-threaded server with thread pool
- ✅ **Socket Communication**: TCP-based client-server communication
- ✅ **Batch Processing**: High-throughput message batching
- ✅ **Compression**: GZIP compression for message values
- ✅ **Metrics Collection**: Performance monitoring and statistics
- ✅ **Health Checking**: System health monitoring
- ✅ **Configuration Management**: External configuration support
- ✅ **Comprehensive Testing**: Unit and integration tests

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   KafkaClient   │    │   KafkaClient   │    │   KafkaClient   │
│   (Producer)    │    │   (Consumer)    │    │   (Admin)       │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │              TCP Socket Communication       │
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼─────────────┐
                    │      KafkaServer          │
                    │  ┌─────────────────────┐  │
                    │  │   Request Handler   │  │
                    │  │   (Thread Pool)     │  │
                    │  └─────────────────────┘  │
                    │  ┌─────────────────────┐  │
                    │  │   TopicManager      │  │
                    │  └─────────────────────┘  │
                    │  ┌─────────────────────┐  │
                    │  │   OffsetManager     │  │
                    │  └─────────────────────┘  │
                    │  ┌─────────────────────┐  │
                    │  │   MetricsCollector  │  │
                    │  └─────────────────────┘  │
                    │  ┌─────────────────────┐  │
                    │  │   HealthChecker     │  │
                    │  └─────────────────────┘  │
                    └───────────────────────────┘
```

### Design Patterns Used

1. **Singleton Pattern**: Used for `MetricsCollector` and `HealthChecker`
2. **Factory Pattern**: Used for creating different types of clients
3. **Observer Pattern**: Used for metrics collection and health monitoring
4. **Strategy Pattern**: Used for different compression algorithms
5. **Command Pattern**: Used for request/response handling
6. **Thread Pool Pattern**: Used for concurrent request processing

---

## Project Structure

```
kafka-java/
├── pom.xml                          # Maven configuration
├── kafka.properties                 # Configuration file
├── README.md                        # Basic project documentation
├── src/
│   ├── main/
│   │   ├── java/com/kafka/
│   │   │   ├── client/              # Client implementations
│   │   │   │   ├── AdminClient.java
│   │   │   │   ├── BatchKafkaProducer.java
│   │   │   │   ├── KafkaConsumer.java
│   │   │   │   └── KafkaProducer.java
│   │   │   ├── config/              # Configuration management
│   │   │   │   └── KafkaConfig.java
│   │   │   ├── examples/            # Usage examples
│   │   │   │   └── SimpleExample.java
│   │   │   ├── health/              # Health monitoring
│   │   │   │   └── HealthChecker.java
│   │   │   ├── metrics/             # Metrics collection
│   │   │   │   └── MetricsCollector.java
│   │   │   ├── protocol/            # Communication protocol
│   │   │   │   ├── BatchMessage.java
│   │   │   │   ├── Message.java
│   │   │   │   ├── Request.java
│   │   │   │   ├── RequestType.java
│   │   │   │   └── Response.java
│   │   │   ├── server/              # Server implementation
│   │   │   │   ├── KafkaServer.java
│   │   │   │   ├── OffsetManager.java
│   │   │   │   ├── Topic.java
│   │   │   │   └── TopicManager.java
│   │   │   └── util/                # Utility classes
│   │   │       ├── CompressionUtils.java
│   │   │       ├── Logger.java
│   │   │       └── NetworkUtils.java
│   │   └── resources/
│   │       └── logback.xml          # Logging configuration
│   └── test/
│       └── java/com/kafka/          # Test classes
│           ├── IntegrationTest.java
│           ├── config/
│           ├── metrics/
│           ├── protocol/
│           └── server/
└── target/                          # Compiled classes
```

---

## Core Components

### 1. Server Components

#### KafkaServer.java

**Purpose**: Main server implementation that handles client connections and request processing.

**Key Responsibilities**:

- Accept client connections via TCP sockets
- Process different types of requests (produce, consume, admin operations)
- Manage thread pool for concurrent request handling
- Coordinate with other server components

**Key Methods**:

- `start()`: Starts the server and begins accepting connections
- `stop()`: Gracefully shuts down the server
- `handleClient(Socket)`: Processes individual client connections
- `processRequest(Request)`: Routes requests to appropriate handlers

**Threading Model**:

- Uses `ExecutorService` with configurable thread pool size
- Each client connection is handled in a separate thread
- Thread-safe operations using concurrent data structures

#### TopicManager.java

**Purpose**: Manages all topics in the broker system.

**Key Responsibilities**:

- Create and manage topics
- Handle message production and consumption
- Provide topic metadata
- Coordinate with individual Topic instances

**Key Methods**:

- `createTopic(String, int)`: Creates a new topic with specified partitions
- `produceMessage(String, Message)`: Produces a message to a topic
- `fetchMessages(String, int, long, int)`: Fetches messages from a topic partition
- `getTopicMetadata(String)`: Returns detailed topic information

#### Topic.java

**Purpose**: Represents a single topic with its partitions and messages.

**Key Responsibilities**:

- Store messages in partitions
- Manage partition-specific offsets
- Handle message production and consumption
- Provide partition-level statistics

**Key Methods**:

- `produceMessage(Message)`: Adds a message to the appropriate partition
- `fetchMessages(int, long, int)`: Retrieves messages from a partition
- `getLatestOffset(int)`: Gets the latest offset for a partition
- `getMessageCount(int)`: Returns the number of messages in a partition

#### OffsetManager.java

**Purpose**: Manages consumer group offsets for tracking consumption progress.

**Key Responsibilities**:

- Track committed offsets for consumer groups
- Manage consumer group membership
- Provide offset-related operations

**Key Methods**:

- `commitOffset(String, String, int, long)`: Commits an offset for a consumer group
- `getCommittedOffset(String, String, int)`: Gets the committed offset
- `registerConsumer(String, String)`: Registers a consumer with a group

### 2. Client Components

#### KafkaProducer.java

**Purpose**: Client for producing messages to topics.

**Key Responsibilities**:

- Connect to the broker
- Send messages to topics
- Handle connection management
- Serialize messages for transmission

**Key Methods**:

- `connect()`: Establishes connection to the broker
- `send(String, String, String)`: Sends a string message
- `send(String, int, byte[], byte[])`: Sends a binary message to specific partition
- `disconnect()`: Closes the connection

#### KafkaConsumer.java

**Purpose**: Client for consuming messages from topics.

**Key Responsibilities**:

- Connect to the broker
- Consume messages from topics
- Manage offset tracking
- Handle consumer group operations

**Key Methods**:

- `consume(String, int, long, int)`: Consumes messages from a topic partition
- `commitOffset(String, int, long)`: Commits the current offset
- `getCommittedOffset(String, int)`: Gets the committed offset

#### BatchKafkaProducer.java

**Purpose**: High-throughput producer that batches messages for better performance.

**Key Responsibilities**:

- Collect messages in batches
- Send batches when size or time threshold is reached
- Apply compression to large messages
- Optimize network usage

**Key Methods**:

- `send(String, String, String)`: Adds a message to the batch
- `flush()`: Forces sending of all pending messages
- `getBatchStatistics()`: Returns batch performance statistics

#### AdminClient.java

**Purpose**: Administrative client for managing topics and cluster operations.

**Key Responsibilities**:

- Create and manage topics
- List available topics
- Get topic metadata
- Perform health checks

**Key Methods**:

- `createTopic(String, int)`: Creates a new topic
- `listTopics()`: Lists all available topics
- `describeTopic(String)`: Gets detailed topic information
- `ping()`: Performs a health check

### 3. Protocol Components

#### Message.java

**Purpose**: Represents a single message in the system.

**Key Features**:

- Binary serialization/deserialization
- CRC32 checksum for data integrity
- Compression support
- Timestamp tracking

**Key Methods**:

- `serialize()`: Converts message to binary format
- `deserialize(byte[])`: Creates message from binary data
- `compressValue()`: Compresses the message value
- `isValid()`: Validates message integrity

#### BatchMessage.java

**Purpose**: Represents a batch of messages for efficient transmission.

**Key Features**:

- Groups multiple messages together
- Batch-level compression
- Integrity validation
- Performance statistics

**Key Methods**:

- `serialize()`: Serializes the entire batch
- `deserialize(byte[])`: Creates batch from binary data
- `getCompressionRatio()`: Calculates compression efficiency

#### Request.java & Response.java

**Purpose**: Define the communication protocol between clients and server.

**Request Types**:

- `PRODUCE`: Send messages to topics
- `FETCH`: Retrieve messages from topics
- `CONSUME`: Consume messages with offset management
- `CREATE_TOPIC`: Create new topics
- `LIST_TOPICS`: List all topics
- `DESCRIBE_TOPIC`: Get topic metadata
- `COMMIT_OFFSET`: Commit consumer offsets
- `GET_OFFSET`: Get committed offsets
- `PING`: Health check
- `GET_METRICS`: Get broker metrics
- `GET_HEALTH`: Get broker health status

### 4. Utility Components

#### NetworkUtils.java

**Purpose**: Handles low-level network operations.

**Key Methods**:

- `readMessage(InputStream)`: Reads a complete message from stream
- `writeMessage(OutputStream, byte[])`: Writes a message to stream
- `sendRequest(Socket, byte[])`: Sends request and receives response
- `isSocketConnected(Socket)`: Checks socket connection status

#### CompressionUtils.java

**Purpose**: Provides compression and decompression functionality.

**Key Methods**:

- `compress(byte[])`: Compresses data using GZIP
- `decompress(byte[])`: Decompresses GZIP data
- `isCompressed(byte[])`: Checks if data is compressed
- `getCompressionRatio(int, int)`: Calculates compression ratio

#### Logger.java

**Purpose**: Centralized logging utility.

**Features**:

- SLF4J integration
- Configurable log levels
- Structured logging
- Performance optimization

### 5. Monitoring Components

#### MetricsCollector.java

**Purpose**: Collects and reports system metrics.

**Key Metrics**:

- Messages produced/consumed
- Bytes produced/consumed
- Request latency
- Error rates
- Topic-specific statistics

**Key Methods**:

- `recordMessageProduced(String, int, int, long)`: Records message production
- `recordMessageConsumed(String, int, int, long)`: Records message consumption
- `getSnapshot()`: Gets current metrics snapshot
- `reset()`: Resets all metrics

#### HealthChecker.java

**Purpose**: Monitors system health and provides health status.

**Health Checks**:

- Memory usage
- Garbage collection performance
- Error rates
- System uptime

**Key Methods**:

- `checkHealth()`: Performs comprehensive health check
- `isHealthy()`: Quick health status check
- `getHealthJson()`: Returns detailed health information

---

## Protocol Design

### Message Format

Each message follows a binary protocol for efficient transmission:

```
┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
│ Topic Length│ Topic Name  │ Partition   │ Offset      │ Timestamp   │ CRC32       │ Compressed  │
│ (4 bytes)   │ (variable)  │ (4 bytes)   │ (8 bytes)   │ (8 bytes)   │ (4 bytes)   │ (1 byte)    │
├─────────────┼─────────────┼─────────────┼─────────────┼─────────────┼─────────────┼─────────────┤
│ Key Length  │ Key Data    │ Value Length│ Value Data  │
│ (4 bytes)   │ (variable)  │ (4 bytes)   │ (variable)  │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
```

### Request/Response Format

```
┌─────────────┬─────────────┬─────────────┬─────────────┐
│ Request Type│ Client ID   │ Data Length │ Request Data│
│ (4 bytes)   │ (variable)  │ (4 bytes)   │ (variable)  │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

### Network Protocol

All network communication uses TCP sockets with length-prefixed messages:

1. **Message Length** (4 bytes): Total size of the message
2. **Message Data** (variable): The actual message content

---

## Features Implementation

### 1. Topic Management

**Implementation**: `TopicManager` + `Topic` classes

**Features**:

- Create topics with configurable partition count
- Automatic partition initialization
- Thread-safe topic operations
- Topic metadata retrieval

**Code Example**:

```java
// Create a topic with 3 partitions
TopicManager topicManager = new TopicManager(3);
Topic topic = topicManager.createTopic("my-topic", 3);

// Get topic metadata
Map<String, Object> metadata = topicManager.getTopicMetadata("my-topic");
```

### 2. Message Production

**Implementation**: `KafkaProducer` + `BatchKafkaProducer`

**Features**:

- Single message production
- Batch message production
- Partition selection
- Compression support
- Connection management

**Code Example**:

```java
// Single message production
KafkaProducer producer = new KafkaProducer("localhost:9092", "producer-1");
producer.connect();
long offset = producer.send("my-topic", "key", "value");

// Batch production
BatchKafkaProducer batchProducer = new BatchKafkaProducer("localhost:9092", "batch-producer");
batchProducer.connect();
batchProducer.send("my-topic", "key1", "value1");
batchProducer.send("my-topic", "key2", "value2");
batchProducer.flush(); // Send all pending messages
```

### 3. Message Consumption

**Implementation**: `KafkaConsumer`

**Features**:

- Message consumption from specific partitions
- Offset management
- Consumer group support
- Batch consumption

**Code Example**:

```java
KafkaConsumer consumer = new KafkaConsumer("localhost:9092", "consumer-1", "group-1");
consumer.connect();

// Consume messages from partition 0
List<Message> messages = consumer.consume("my-topic", 0, 0, 10);

// Commit offset
consumer.commitOffset("my-topic", 0, messages.size() - 1);
```

### 4. Offset Management

**Implementation**: `OffsetManager`

**Features**:

- Consumer group offset tracking
- Offset commitment
- Offset retrieval
- Group membership management

**Code Example**:

```java
OffsetManager offsetManager = new OffsetManager();

// Commit offset
offsetManager.commitOffset("group-1", "my-topic", 0, 100);

// Get committed offset
long offset = offsetManager.getCommittedOffset("group-1", "my-topic", 0);
```

### 5. Compression

**Implementation**: `CompressionUtils` + `Message` compression

**Features**:

- GZIP compression
- Automatic compression for large messages
- Compression ratio calculation
- Transparent decompression

**Code Example**:

```java
// Compress data
byte[] originalData = "Hello, World!".getBytes();
byte[] compressedData = CompressionUtils.compress(originalData);

// Decompress data
byte[] decompressedData = CompressionUtils.decompress(compressedData);

// Check compression ratio
double ratio = CompressionUtils.getCompressionRatio(originalData.length, compressedData.length);
```

### 6. Metrics Collection

**Implementation**: `MetricsCollector`

**Features**:

- Real-time metrics collection
- Topic-specific metrics
- Performance statistics
- Periodic reporting

**Code Example**:

```java
MetricsCollector metrics = MetricsCollector.getInstance();

// Record metrics
metrics.recordMessageProduced("my-topic", 0, 100, 5);
metrics.recordMessageConsumed("my-topic", 0, 100, 3);

// Get metrics snapshot
MetricsCollector.MetricsSnapshot snapshot = metrics.getSnapshot();
System.out.println("Messages produced: " + snapshot.getMessagesProduced());
```

### 7. Health Monitoring

**Implementation**: `HealthChecker`

**Features**:

- Memory usage monitoring
- Garbage collection tracking
- Error rate calculation
- System uptime tracking

**Code Example**:

```java
HealthChecker healthChecker = HealthChecker.getInstance();

// Check health
HealthChecker.HealthStatus status = healthChecker.checkHealth();
if (status.isHealthy()) {
    System.out.println("System is healthy");
} else {
    System.out.println("Issues: " + status.getIssues());
}

// Get health JSON
String healthJson = healthChecker.getHealthJson();
```

---

## Configuration System

### Configuration File: `kafka.properties`

The system uses a properties file for configuration management:

```properties
# Server Configuration
server.port=9092
server.host=0.0.0.0
server.thread.pool.size=10
server.max.connections=100

# Topic Configuration
topic.default.partitions=3
topic.max.name.length=255
topic.cleanup.interval.ms=300000

# Message Configuration
message.max.size.bytes=1048576
message.retention.ms=604800000
message.compression.enabled=true
message.compression.threshold.bytes=1024

# Network Configuration
network.socket.timeout.ms=30000
network.connection.timeout.ms=10000
network.receive.buffer.size.bytes=65536
network.send.buffer.size.bytes=65536

# Logging Configuration
logging.level=INFO
logging.file.max.size.mb=100
logging.file.max.history.days=30

# Performance Configuration
performance.batch.size=1000
performance.batch.timeout.ms=1000
performance.metrics.enabled=true
performance.metrics.interval.ms=60000
```

### Configuration Management: `KafkaConfig.java`

**Features**:

- Property file loading
- Default value fallback
- Type-safe configuration access
- Runtime configuration updates

**Usage**:

```java
// Get configuration values
int port = KafkaConfig.getInt(KafkaConfig.SERVER_PORT, 9092);
String host = KafkaConfig.getString(KafkaConfig.SERVER_HOST, "localhost");
boolean compressionEnabled = KafkaConfig.getBoolean(KafkaConfig.ENABLE_COMPRESSION, true);

// Set configuration values
KafkaConfig.setProperty("server.port", "9093");

// Reload configuration
KafkaConfig.reload();
```

---

## Client APIs

### 1. Producer API

#### Basic Producer

```java
// Create producer
KafkaProducer producer = new KafkaProducer("localhost:9092", "my-producer");

// Connect
producer.connect();

// Send messages
long offset1 = producer.send("my-topic", "key1", "value1");
long offset2 = producer.send("my-topic", 1, "key2", "value2"); // Specific partition

// Disconnect
producer.disconnect();
```

#### Batch Producer

```java
// Create batch producer
BatchKafkaProducer batchProducer = new BatchKafkaProducer("localhost:9092", "batch-producer");

// Connect
batchProducer.connect();

// Send messages (will be batched)
batchProducer.send("my-topic", "key1", "value1");
batchProducer.send("my-topic", "key2", "value2");
batchProducer.send("my-topic", "key3", "value3");

// Force send all pending messages
batchProducer.flush();

// Disconnect
batchProducer.disconnect();
```

### 2. Consumer API

```java
// Create consumer
KafkaConsumer consumer = new KafkaConsumer("localhost:9092", "my-consumer", "my-group");

// Connect
consumer.connect();

// Consume messages
List<Message> messages = consumer.consume("my-topic", 0, 0, 10);
for (Message message : messages) {
    System.out.println("Received: " + new String(message.getValue()));
}

// Commit offset
consumer.commitOffset("my-topic", 0, messages.size() - 1);

// Get committed offset
long offset = consumer.getCommittedOffset("my-topic", 0);

// Disconnect
consumer.disconnect();
```

### 3. Admin API

```java
// Create admin client
AdminClient admin = new AdminClient("localhost:9092", "admin");

// Connect
admin.connect();

// Create topic
admin.createTopic("my-topic", 3);

// List topics
List<String> topics = admin.listTopics();

// Get topic metadata
String metadata = admin.describeTopic("my-topic");

// Health check
boolean isHealthy = admin.ping();

// Disconnect
admin.disconnect();
```

---

## Server Implementation

### Server Lifecycle

1. **Initialization**: Load configuration, create managers
2. **Startup**: Bind to port, start thread pool
3. **Operation**: Accept connections, process requests
4. **Shutdown**: Graceful shutdown of all components

### Request Processing Flow

```
Client Request → Socket → Thread Pool → Request Handler → Component → Response → Client
```

### Threading Model

- **Main Thread**: Accepts client connections
- **Worker Threads**: Process individual client requests
- **Background Threads**: Handle metrics collection, health checks

### Error Handling

- **Connection Errors**: Graceful disconnection
- **Request Errors**: Error responses with details
- **System Errors**: Logging and recovery

---

## Testing Strategy

### 1. Unit Tests

**Purpose**: Test individual components in isolation

**Test Files**:

- `KafkaConfigTest.java`: Configuration management
- `MessageTest.java`: Message serialization/deserialization
- `BatchMessageTest.java`: Batch message functionality
- `CompressionUtilsTest.java`: Compression utilities
- `MetricsCollectorTest.java`: Metrics collection
- `OffsetManagerTest.java`: Offset management
- `TopicTest.java`: Topic operations
- `TopicManagerTest.java`: Topic management

### 2. Integration Tests

**Purpose**: Test end-to-end functionality

**Test File**: `IntegrationTest.java`

**Test Cases**:

- End-to-end message flow
- Offset management
- Multiple consumers
- Server ping
- Topic metadata

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=IntegrationTest

# Run with verbose output
mvn test -Dtest=IntegrationTest -X
```

---

## Getting Started Guide

### Prerequisites

- **Java 17** or higher
- **Maven 3.6** or higher
- **Git** (for cloning the repository)

### Installation

1. **Clone the repository**:

```bash
git clone <repository-url>
cd kafka-java
```

2. **Build the project**:

```bash
mvn clean compile
```

3. **Run tests**:

```bash
mvn test
```

### Quick Start

1. **Start the server**:

```bash
mvn exec:java -Dexec.mainClass="com.kafka.server.KafkaServer"
```

2. **Run the example** (in another terminal):

```bash
mvn exec:java -Dexec.mainClass="com.kafka.examples.SimpleExample"
```

### Basic Usage Example

```java
public class MyKafkaApp {
    public static void main(String[] args) throws IOException {
        // Create admin client
        AdminClient admin = new AdminClient("localhost:9092", "admin");
        admin.connect();

        // Create topic
        admin.createTopic("my-topic", 3);

        // Create producer
        KafkaProducer producer = new KafkaProducer("localhost:9092", "producer");
        producer.connect();

        // Send messages
        producer.send("my-topic", "key1", "Hello, Kafka!");
        producer.send("my-topic", "key2", "This is a test message");

        // Create consumer
        KafkaConsumer consumer = new KafkaConsumer("localhost:9092", "consumer", "group1");
        consumer.connect();

        // Consume messages
        List<Message> messages = consumer.consume("my-topic", 0, 0, 10);
        for (Message message : messages) {
            System.out.println("Received: " + new String(message.getValue()));
        }

        // Clean up
        consumer.disconnect();
        producer.disconnect();
        admin.disconnect();
    }
}
```

---

## Advanced Usage

### 1. Custom Configuration

```java
// Set custom configuration
KafkaConfig.setProperty("server.port", "9093");
KafkaConfig.setProperty("topic.default.partitions", "5");
KafkaConfig.setProperty("message.compression.enabled", "true");

// Reload configuration
KafkaConfig.reload();
```

### 2. Batch Processing

```java
// Configure batch producer
BatchKafkaProducer batchProducer = new BatchKafkaProducer("localhost:9092", "batch-producer");

// Send many messages efficiently
for (int i = 0; i < 1000; i++) {
    batchProducer.send("my-topic", "key" + i, "value" + i);
}

// Force send all pending messages
batchProducer.flush();
```

### 3. Compression

```java
// Enable compression for large messages
Message message = new Message("my-topic", 0, 0, "key".getBytes(), largeValue);
message.compressValue();

// Check if message is compressed
if (message.isCompressed()) {
    byte[] decompressed = message.getDecompressedValue();
}
```

### 4. Metrics Monitoring

```java
// Get metrics collector
MetricsCollector metrics = MetricsCollector.getInstance();

// Get current metrics
MetricsCollector.MetricsSnapshot snapshot = metrics.getSnapshot();
System.out.println("Messages produced: " + snapshot.getMessagesProduced());
System.out.println("Average latency: " + snapshot.getAverageProduceLatency() + "ms");

// Reset metrics
metrics.reset();
```

### 5. Health Monitoring

```java
// Get health checker
HealthChecker healthChecker = HealthChecker.getInstance();

// Check health
HealthChecker.HealthStatus status = healthChecker.checkHealth();
if (!status.isHealthy()) {
    System.out.println("Health issues: " + status.getIssues());
}

// Get health JSON
String healthJson = healthChecker.getHealthJson();
System.out.println(healthJson);
```

---

## Performance Considerations

### 1. Threading

- **Server Thread Pool**: Configurable size (default: 10 threads)
- **Client Connections**: One thread per connection
- **Background Tasks**: Separate threads for metrics and health checks

### 2. Memory Management

- **Message Storage**: In-memory storage (not persistent)
- **Batch Processing**: Reduces memory allocation overhead
- **Compression**: Reduces memory usage for large messages

### 3. Network Optimization

- **Batch Messages**: Reduces network round trips
- **Compression**: Reduces network bandwidth usage
- **Binary Protocol**: Efficient serialization

### 4. Configuration Tuning

```properties
# Increase thread pool for high concurrency
server.thread.pool.size=20

# Increase batch size for high throughput
performance.batch.size=5000

# Enable compression for large messages
message.compression.enabled=true
message.compression.threshold.bytes=512

# Increase buffer sizes for better network performance
network.receive.buffer.size.bytes=131072
network.send.buffer.size.bytes=131072
```

---

## Troubleshooting

### Common Issues

#### 1. Connection Refused

**Error**: `java.net.ConnectException: Connection refused`

**Solutions**:

- Ensure server is running
- Check port configuration
- Verify firewall settings

#### 2. Topic Not Found

**Error**: `Topic not found: my-topic`

**Solutions**:

- Create topic before using it
- Check topic name spelling
- Verify topic exists using AdminClient

#### 3. Invalid Partition

**Error**: `Invalid partition: 5`

**Solutions**:

- Check topic partition count
- Use valid partition numbers (0 to partitionCount-1)
- Verify topic configuration

#### 4. Memory Issues

**Error**: `OutOfMemoryError`

**Solutions**:

- Increase JVM heap size: `-Xmx2g`
- Reduce batch size
- Enable compression
- Monitor memory usage

### Debugging

#### 1. Enable Debug Logging

```properties
# In kafka.properties
logging.level=DEBUG
```

#### 2. Check Server Status

```java
AdminClient admin = new AdminClient("localhost:9092", "admin");
admin.connect();
boolean isHealthy = admin.ping();
System.out.println("Server healthy: " + isHealthy);
```

#### 3. Monitor Metrics

```java
MetricsCollector metrics = MetricsCollector.getInstance();
MetricsCollector.MetricsSnapshot snapshot = metrics.getSnapshot();
System.out.println("Error count: " + snapshot.getErrorsCount());
```

---

## API Reference

### Server Classes

#### KafkaServer

```java
public class KafkaServer {
    public KafkaServer(int port)
    public void start() throws IOException
    public void stop()
    public static void main(String[] args)
}
```

#### TopicManager

```java
public class TopicManager {
    public TopicManager(int defaultPartitionCount)
    public Topic createTopic(String name, int partitionCount)
    public Topic createTopic(String name)
    public Topic getTopic(String name)
    public boolean topicExists(String name)
    public Set<String> listTopics()
    public long produceMessage(String topicName, Message message)
    public List<Message> fetchMessages(String topicName, int partition, long offset, int maxMessages)
    public long getLatestOffset(String topicName, int partition)
    public long getEarliestOffset(String topicName, int partition)
    public Map<String, Object> getTopicMetadata(String topicName)
    public int getTopicCount()
}
```

#### Topic

```java
public class Topic {
    public Topic(String name, int partitionCount)
    public long produceMessage(Message message)
    public List<Message> fetchMessages(int partition, long offset, int maxMessages)
    public long getLatestOffset(int partition)
    public long getEarliestOffset(int partition)
    public int getMessageCount(int partition)
    public String getName()
    public int getPartitionCount()
    public long getCreatedAt()
}
```

#### OffsetManager

```java
public class OffsetManager {
    public OffsetManager()
    public void commitOffset(String groupId, String topicName, int partition, long offset)
    public long getCommittedOffset(String groupId, String topicName, int partition)
    public void registerConsumer(String groupId, String consumerId)
    public void unregisterConsumer(String groupId, String consumerId)
    public Set<String> getConsumerGroups()
    public Set<String> getConsumersInGroup(String groupId)
    public Map<String, Map<Integer, Long>> getAllOffsets(String groupId)
    public void removeGroup(String groupId)
    public int getGroupCount()
}
```

### Client Classes

#### KafkaProducer

```java
public class KafkaProducer {
    public KafkaProducer(String bootstrapServers, String clientId)
    public void connect() throws IOException
    public void disconnect()
    public long send(String topic, byte[] key, byte[] value) throws IOException
    public long send(String topic, int partition, byte[] key, byte[] value) throws IOException
    public long send(String topic, String key, String value) throws IOException
    public long send(String topic, int partition, String key, String value) throws IOException
    public boolean isConnected()
    public String getClientId()
}
```

#### KafkaConsumer

```java
public class KafkaConsumer {
    public KafkaConsumer(String bootstrapServers, String clientId, String groupId)
    public void connect() throws IOException
    public void disconnect()
    public List<Message> consume(String topic, int partition, long offset, int maxMessages) throws IOException
    public List<Message> consume(String topic, long offset, int maxMessages) throws IOException
    public void commitOffset(String topic, int partition, long offset) throws IOException
    public long getCommittedOffset(String topic, int partition) throws IOException
    public boolean isConnected()
    public String getClientId()
    public String getGroupId()
}
```

#### BatchKafkaProducer

```java
public class BatchKafkaProducer {
    public BatchKafkaProducer(String bootstrapServers, String clientId)
    public void connect() throws IOException
    public void disconnect()
    public void send(String topic, byte[] key, byte[] value) throws IOException
    public void send(String topic, int partition, byte[] key, byte[] value) throws IOException
    public void send(String topic, String key, String value) throws IOException
    public void send(String topic, int partition, String key, String value) throws IOException
    public void flush() throws IOException
    public boolean isConnected()
    public String getClientId()
    public int getPendingMessageCount()
    public String getBatchStatistics()
}
```

#### AdminClient

```java
public class AdminClient {
    public AdminClient(String bootstrapServers, String clientId)
    public void connect() throws IOException
    public void disconnect()
    public void createTopic(String topicName, int partitionCount) throws IOException
    public void createTopic(String topicName) throws IOException
    public List<String> listTopics() throws IOException
    public String describeTopic(String topicName) throws IOException
    public boolean ping() throws IOException
    public boolean isConnected()
    public String getClientId()
}
```

### Protocol Classes

#### Message

```java
public class Message {
    public Message(String topic, int partition, long offset, byte[] key, byte[] value)
    public static Message createCompressed(String topic, int partition, long offset, byte[] key, byte[] value) throws IOException
    public static Message deserialize(byte[] data)
    public byte[] serialize()
    public void compressValue() throws IOException
    public void decompressValue() throws IOException
    public byte[] getDecompressedValue() throws IOException
    public boolean isValid()
    public String getTopic()
    public int getPartition()
    public long getOffset()
    public byte[] getKey()
    public byte[] getValue()
    public long getTimestamp()
    public int getCrc32()
    public boolean isCompressed()
}
```

#### BatchMessage

```java
public class BatchMessage {
    public BatchMessage(String topic, int partition, List<Message> messages)
    public static BatchMessage deserialize(byte[] data) throws IOException
    public byte[] serialize() throws IOException
    public boolean isValid()
    public int getTotalSize()
    public double getAverageMessageSize()
    public double getCompressionRatio()
    public String getTopic()
    public int getPartition()
    public List<Message> getMessages()
    public long getTimestamp()
    public String getBatchId()
    public int getCrc32()
    public int getMessageCount()
}
```

#### Request

```java
public class Request {
    public Request(RequestType type, byte[] data, String clientId)
    public static Request deserialize(byte[] data)
    public byte[] serialize()
    public RequestType getType()
    public byte[] getData()
    public String getClientId()
}
```

#### Response

```java
public class Response {
    public Response(Status status, byte[] data)
    public Response(Status status, String errorMessage)
    public static Response deserialize(byte[] rawData)
    public byte[] serialize()
    public Status getStatus()
    public byte[] getData()
    public String getErrorMessage()
    public long getTimestamp()
    public boolean isSuccess()
}
```

### Utility Classes

#### NetworkUtils

```java
public class NetworkUtils {
    public static byte[] readMessage(InputStream inputStream) throws IOException
    public static void writeMessage(OutputStream outputStream, byte[] message) throws IOException
    public static byte[] sendRequest(Socket socket, byte[] requestData) throws IOException
    public static void closeSocket(Socket socket)
    public static boolean isSocketConnected(Socket socket)
}
```

#### CompressionUtils

```java
public class CompressionUtils {
    public static byte[] compress(byte[] data) throws IOException
    public static byte[] decompress(byte[] compressedData) throws IOException
    public static boolean isCompressed(byte[] data)
    public static double getCompressionRatio(int originalSize, int compressedSize)
}
```

#### KafkaConfig

```java
public class KafkaConfig {
    public static String getString(String key)
    public static String getString(String key, String defaultValue)
    public static int getInt(String key)
    public static int getInt(String key, int defaultValue)
    public static long getLong(String key)
    public static long getLong(String key, long defaultValue)
    public static boolean getBoolean(String key)
    public static boolean getBoolean(String key, boolean defaultValue)
    public static void setProperty(String key, String value)
    public static boolean isLoaded()
    public static Properties getAllProperties()
    public static void reload()
    public static void printConfiguration()
}
```

### Monitoring Classes

#### MetricsCollector

```java
public class MetricsCollector {
    public static MetricsCollector getInstance()
    public void recordMessageProduced(String topic, int partition, int messageSize, long latencyMs)
    public void recordMessageConsumed(String topic, int partition, int messageSize, long latencyMs)
    public void recordRequest(String requestType, long latencyMs)
    public void recordError(String errorType)
    public MetricsSnapshot getSnapshot()
    public void reset()
    public void shutdown()
}
```

#### HealthChecker

```java
public class HealthChecker {
    public static HealthChecker getInstance()
    public HealthStatus checkHealth()
    public boolean isHealthy()
    public String getHealthJson()
}
```

---

## Conclusion

This Kafka-like message broker implementation demonstrates the core concepts of distributed messaging systems in a simplified, educational context. While it lacks many production features like persistence, replication, and clustering, it provides a solid foundation for understanding:

- **Message Broker Architecture**: How message brokers work internally
- **Concurrent Programming**: Thread-safe operations and thread pools
- **Network Programming**: TCP socket communication and protocols
- **Data Serialization**: Binary protocols and message formats
- **System Monitoring**: Metrics collection and health checking
- **Configuration Management**: External configuration and defaults
- **Testing Strategies**: Unit and integration testing approaches

The codebase is well-structured, thoroughly documented, and includes comprehensive tests, making it an excellent learning resource for understanding distributed systems concepts.

---

_This documentation covers the complete implementation of the Kafka-like message broker. For questions or contributions, please refer to the project repository._
