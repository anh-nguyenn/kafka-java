package com.kafka.protocol;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents a request sent from client to server.
 * Contains the request type and associated data.
 */
public class Request {
    private final RequestType type;
    private final byte[] data;
    private final String clientId;
    private long timestamp;

    public Request(RequestType type, byte[] data, String clientId) {
        this.type = Objects.requireNonNull(type, "Request type cannot be null");
        this.data = data != null ? data : new byte[0];
        this.clientId = clientId != null ? clientId : "unknown";
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Deserializes a request from bytes
     */
    public static Request deserialize(byte[] rawData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);
        
        // Read request type
        int typeCode = buffer.getInt();
        RequestType type = RequestType.fromCode(typeCode);
        
        // Read client ID length and client ID
        int clientIdLength = buffer.getInt();
        byte[] clientIdBytes = new byte[clientIdLength];
        buffer.get(clientIdBytes);
        String clientId = new String(clientIdBytes);
        
        // Read timestamp
        long timestamp = buffer.getLong();
        
        // Read data length and data
        int dataLength = buffer.getInt();
        byte[] data = new byte[dataLength];
        buffer.get(data);
        
        Request request = new Request(type, data, clientId);
        request.timestamp = timestamp;
        return request;
    }

    /**
     * Serializes the request to bytes
     */
    public byte[] serialize() {
        byte[] clientIdBytes = clientId.getBytes();
        int totalSize = 4 + 4 + clientIdBytes.length + 8 + 4 + data.length;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write request type
        buffer.putInt(type.getCode());
        
        // Write client ID
        buffer.putInt(clientIdBytes.length);
        buffer.put(clientIdBytes);
        
        // Write timestamp
        buffer.putLong(timestamp);
        
        // Write data
        buffer.putInt(data.length);
        buffer.put(data);
        
        return buffer.array();
    }

    // Getters
    public RequestType getType() { return type; }
    public byte[] getData() { return data; }
    public String getClientId() { return clientId; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("Request{type=%s, clientId='%s', dataLength=%d, timestamp=%d}",
                type, clientId, data.length, timestamp);
    }
}
