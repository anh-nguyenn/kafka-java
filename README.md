# Kafka-like Message Broker in Java

A simplified implementation of a Kafka-like message broker built in Java 17. This project demonstrates core concepts of distributed messaging systems including topics, partitions, producers, consumers, and offset management.

## Features

- **Topic Management**: Create and manage topics with configurable partitions
- **Message Production**: Send messages to topics with key-value pairs
- **Message Consumption**: Consume messages from topics with offset tracking
- **Partitioning**: Distribute messages across multiple partitions
- **Offset Management**: Track consumer group progress
- **Binary Protocol**: Efficient message serialization/deserialization
- **Concurrent Processing**: Multi-threaded server with thread pool
- **Socket Communication**: TCP-based client-server communication

## Architecture

```
src/main/java/com/kafka/
├── server/          # Server-side components
│   ├── KafkaServer.java      # Main server implementation
│   ├── Topic.java            # Topic and partition management
│   ├── TopicManager.java     # Topic lifecycle management
│   └── OffsetManager.java    # Consumer offset tracking
├── client/          # Client-side components
│   ├── KafkaProducer.java    # Message producer
│   ├── KafkaConsumer.java    # Message consumer
│   └── AdminClient.java      # Administrative operations
├── protocol/        # Communication protocol
│   ├── Message.java          # Message data structure
│   ├── Request.java          # Client requests
│   ├── Response.java         # Server responses
│   └── RequestType.java      # Request type enumeration
├── util/            # Utility classes
│   ├── NetworkUtils.java     # Network operations
│   └── Logger.java           # Logging utilities
└── examples/        # Usage examples
    └── SimpleExample.java    # Basic usage example
```

## Quick Start

### Prerequisites

- Java 17 or higher
- Maven 3.6 or higher

### Building the Project

```bash
mvn clean compile
```

### Running the Server

```bash
mvn exec:java -Dexec.mainClass="com.kafka.server.KafkaServer" -Dexec.args="9092"
```

### Running Tests

```bash
mvn test
```

### Running the Example

```bash
# Terminal 1: Start the server
mvn exec:java -Dexec.mainClass="com.kafka.server.KafkaServer"

# Terminal 2: Run the example
mvn exec:java -Dexec.mainClass="com.kafka.examples.SimpleExample"
```

## Usage Examples

### Creating a Producer

```java
KafkaProducer producer = new KafkaProducer("localhost:9092", "my-producer");
producer.connect();

// Send a message
long offset = producer.send("my-topic", "key", "value");

producer.disconnect();
```

### Creating a Consumer

```java
KafkaConsumer consumer = new KafkaConsumer("localhost:9092", "my-consumer", "my-group");
consumer.connect();

// Consume messages
List<Message> messages = consumer.consume("my-topic", 0, 0, 10);
for (Message message : messages) {
    System.out.println("Received: " + new String(message.getValue()));
}

consumer.disconnect();
```

### Administrative Operations

```java
AdminClient admin = new AdminClient("localhost:9092", "admin");
admin.connect();

// Create a topic
admin.createTopic("my-topic", 3);

// List topics
List<String> topics = admin.listTopics();

// Get topic metadata
String metadata = admin.describeTopic("my-topic");

admin.disconnect();
```

## Protocol Details

The broker uses a binary protocol for efficient communication:

### Message Format

- Topic name (length + bytes)
- Partition number (4 bytes)
- Offset (8 bytes)
- Timestamp (8 bytes)
- CRC32 checksum (4 bytes)
- Key (length + bytes, optional)
- Value (length + bytes)

### Request Types

- `PRODUCE`: Send messages to topics
- `FETCH`: Retrieve messages from topics
- `CONSUME`: Consume messages with offset management
- `CREATE_TOPIC`: Create new topics
- `LIST_TOPICS`: List all topics
- `DESCRIBE_TOPIC`: Get topic metadata
- `COMMIT_OFFSET`: Commit consumer offsets
- `GET_OFFSET`: Get committed offsets
- `PING`: Health check

## Configuration

### Server Configuration

- **Port**: Default 9092 (configurable via command line)
- **Thread Pool Size**: 10 threads
- **Default Partitions**: 3 per topic
- **Message Size Limit**: 1MB

### Logging

The project uses SLF4J with Logback for logging. Logs are written to both console and `logs/kafka-broker.log`.

## Testing

The project includes comprehensive unit tests and integration tests:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test end-to-end message flow
- **Protocol Tests**: Test message serialization/deserialization

Run tests with:

```bash
mvn test
```

## Design Decisions

### Threading Model

- **Server**: Uses a thread pool to handle multiple clients concurrently
- **Clients**: Synchronous operations for simplicity
- **Message Storage**: Thread-safe collections for concurrent access

### Message Storage

- **In-Memory**: Messages are stored in memory for simplicity
- **Partitioning**: Each partition is a separate list of messages
- **Offset Management**: Atomic counters for offset generation

### Error Handling

- **Graceful Degradation**: Server continues running even if individual requests fail
- **Client Resilience**: Clients handle connection failures and retry
- **Validation**: Input validation at protocol and business logic levels

## Limitations

This is a simplified implementation and has several limitations compared to production Kafka:

- **Persistence**: Messages are not persisted to disk
- **Replication**: No data replication or fault tolerance
- **Scaling**: Single server instance only
- **Performance**: Not optimized for high-throughput scenarios
- **Security**: No authentication or authorization
- **Compression**: No message compression
- **Batching**: No message batching for efficiency

## Future Enhancements

Potential improvements for a production system:

1. **Persistence**: Add disk-based storage with WAL
2. **Replication**: Implement leader-follower replication
3. **Clustering**: Support multiple broker nodes
4. **Security**: Add authentication and encryption
5. **Monitoring**: Add metrics and health checks
6. **Configuration**: External configuration management
7. **Performance**: Optimize for high-throughput scenarios

## License

This project is for educational purposes and demonstrates core concepts of distributed messaging systems.
