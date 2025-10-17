package com.kafka.protocol;

/**
 * Enumeration of supported request types in the Kafka-like protocol.
 * Defines the different operations that clients can perform.
 */
public enum RequestType {
    // Producer operations
    PRODUCE(1, "Produce messages to a topic"),
    PRODUCE_BATCH(2, "Produce batch of messages to a topic"),
    
    // Consumer operations
    FETCH(3, "Fetch messages from a topic"),
    CONSUME(4, "Consume messages with offset management"),
    
    // Topic management
    CREATE_TOPIC(5, "Create a new topic"),
    LIST_TOPICS(6, "List all available topics"),
    DESCRIBE_TOPIC(7, "Get topic metadata"),
    
    // Offset management
    COMMIT_OFFSET(8, "Commit consumer offset"),
    GET_OFFSET(9, "Get current offset for consumer group"),
    
    // Health check
    PING(10, "Health check ping");

    private final int code;
    private final String description;

    RequestType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Get RequestType by code
     */
    public static RequestType fromCode(int code) {
        for (RequestType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown request type code: " + code);
    }
}
