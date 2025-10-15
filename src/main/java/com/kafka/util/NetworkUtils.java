package com.kafka.util;

import java.io.*;
import java.net.Socket;

/**
 * Utility class for network operations and data transmission.
 * Handles reading/writing data over sockets with proper length prefixes.
 */
public class NetworkUtils {
    
    /**
     * Reads a complete message from the input stream.
     * The message is prefixed with its length (4 bytes).
     */
    public static byte[] readMessage(InputStream inputStream) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        
        // Read message length
        int messageLength = dataInputStream.readInt();
        if (messageLength < 0 || messageLength > 1024 * 1024) { // 1MB limit
            throw new IOException("Invalid message length: " + messageLength);
        }
        
        // Read message data
        byte[] message = new byte[messageLength];
        dataInputStream.readFully(message);
        
        return message;
    }
    
    /**
     * Writes a complete message to the output stream.
     * The message is prefixed with its length (4 bytes).
     */
    public static void writeMessage(OutputStream outputStream, byte[] message) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        
        // Write message length
        dataOutputStream.writeInt(message.length);
        
        // Write message data
        dataOutputStream.write(message);
        dataOutputStream.flush();
    }
    
    /**
     * Sends a request and receives a response over a socket.
     */
    public static byte[] sendRequest(Socket socket, byte[] requestData) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();
        
        // Send request
        writeMessage(outputStream, requestData);
        
        // Read response
        return readMessage(inputStream);
    }
    
    /**
     * Closes socket resources safely
     */
    public static void closeSocket(Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e) {
                // Log error but don't throw
                System.err.println("Error closing socket: " + e.getMessage());
            }
        }
    }
    
    /**
     * Checks if a socket is still connected
     */
    public static boolean isSocketConnected(Socket socket) {
        return socket != null && !socket.isClosed() && socket.isConnected();
    }
}
