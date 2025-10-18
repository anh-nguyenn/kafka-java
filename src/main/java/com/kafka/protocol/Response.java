package com.kafka.protocol;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents a response sent from server to client.
 * Contains status information and response data.
 */
public class Response {
    public enum Status {
        SUCCESS(200, "Success"),
        BAD_REQUEST(400, "Bad Request"),
        NOT_FOUND(404, "Not Found"),
        INTERNAL_ERROR(500, "Internal Server Error"),
        TOPIC_NOT_FOUND(1001, "Topic Not Found"),
        INVALID_PARTITION(1002, "Invalid Partition"),
        OFFSET_OUT_OF_RANGE(1003, "Offset Out of Range");

        private final int code;
        private final String message;

        Status(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public int getCode() { return code; }
        public String getMessage() { return message; }
        
        /**
         * Get Status by code
         */
        public static Status fromCode(int code) {
            for (Status status : values()) {
                if (status.code == code) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Unknown status code: " + code);
        }
    }

    private final Status status;
    private final byte[] data;
    private String errorMessage;
    private long timestamp;

    public Response(Status status, byte[] data) {
        this.status = Objects.requireNonNull(status, "Status cannot be null");
        this.data = data != null ? data : new byte[0];
        this.errorMessage = null;
        this.timestamp = System.currentTimeMillis();
    }

    public Response(Status status, String errorMessage) {
        this.status = Objects.requireNonNull(status, "Status cannot be null");
        this.data = new byte[0];
        this.errorMessage = errorMessage;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Deserializes a response from bytes
     */
    public static Response deserialize(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);
        
        // Read status code
        int statusCode = buffer.getInt();
        Status status = Status.fromCode(statusCode);
        
        // Read timestamp
        long timestamp = buffer.getLong();
        
        // Read error message length and error message
        int errorMessageLength = buffer.getInt();
        String errorMessage = null;
        if (errorMessageLength > 0) {
            byte[] errorMessageBytes = new byte[errorMessageLength];
            buffer.get(errorMessageBytes);
            errorMessage = new String(errorMessageBytes);
        }
        
        // Read data length and data
        int dataLength = buffer.getInt();
        byte[] data = new byte[dataLength];
        buffer.get(data);
        
        Response response = new Response(status, data);
        response.timestamp = timestamp;
        response.errorMessage = errorMessage;
        return response;
    }

    /**
     * Serializes the response to bytes
     */
    public byte[] serialize() {
        byte[] errorMessageBytes = errorMessage != null ? errorMessage.getBytes() : new byte[0];
        int totalSize = 4 + 8 + 4 + errorMessageBytes.length + 4 + data.length;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write status code
        buffer.putInt(status.getCode());
        
        // Write timestamp
        buffer.putLong(timestamp);
        
        // Write error message
        buffer.putInt(errorMessageBytes.length);
        if (errorMessageBytes.length > 0) {
            buffer.put(errorMessageBytes);
        }
        
        // Write data
        buffer.putInt(data.length);
        buffer.put(data);
        
        return buffer.array();
    }

    // Getters
    public Status getStatus() { return status; }
    public byte[] getData() { return data; }
    public String getErrorMessage() { return errorMessage; }
    public long getTimestamp() { return timestamp; }

    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }

    @Override
    public String toString() {
        return String.format("Response{status=%s, dataLength=%d, errorMessage='%s', timestamp=%d}",
                status, data.length, errorMessage, timestamp);
    }
}
