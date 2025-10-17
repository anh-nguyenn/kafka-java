package com.kafka.config;

import com.kafka.util.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration management for the Kafka-like broker.
 * Loads configuration from properties file with sensible defaults.
 */
public class KafkaConfig {
    private static final String CONFIG_FILE = "kafka.properties";
    private static final Properties properties = new Properties();
    private static boolean loaded = false;

    // Server configuration
    public static final String SERVER_PORT = "server.port";
    public static final String SERVER_HOST = "server.host";
    public static final String THREAD_POOL_SIZE = "server.thread.pool.size";
    public static final String MAX_CONNECTIONS = "server.max.connections";
    
    // Topic configuration
    public static final String DEFAULT_PARTITIONS = "topic.default.partitions";
    public static final String MAX_TOPIC_NAME_LENGTH = "topic.max.name.length";
    public static final String TOPIC_CLEANUP_INTERVAL = "topic.cleanup.interval.ms";
    
    // Message configuration
    public static final String MAX_MESSAGE_SIZE = "message.max.size.bytes";
    public static final String MESSAGE_RETENTION_MS = "message.retention.ms";
    public static final String ENABLE_COMPRESSION = "message.compression.enabled";
    public static final String COMPRESSION_THRESHOLD = "message.compression.threshold.bytes";
    
    // Network configuration
    public static final String SOCKET_TIMEOUT_MS = "network.socket.timeout.ms";
    public static final String CONNECTION_TIMEOUT_MS = "network.connection.timeout.ms";
    public static final String RECEIVE_BUFFER_SIZE = "network.receive.buffer.size.bytes";
    public static final String SEND_BUFFER_SIZE = "network.send.buffer.size.bytes";
    
    // Logging configuration
    public static final String LOG_LEVEL = "logging.level";
    public static final String LOG_FILE_MAX_SIZE = "logging.file.max.size.mb";
    public static final String LOG_FILE_MAX_HISTORY = "logging.file.max.history.days";
    
    // Performance configuration
    public static final String BATCH_SIZE = "performance.batch.size";
    public static final String BATCH_TIMEOUT_MS = "performance.batch.timeout.ms";
    public static final String ENABLE_METRICS = "performance.metrics.enabled";
    public static final String METRICS_INTERVAL_MS = "performance.metrics.interval.ms";

    static {
        loadConfiguration();
    }

    /**
     * Loads configuration from properties file
     */
    private static void loadConfiguration() {
        try (InputStream input = new FileInputStream(CONFIG_FILE)) {
            properties.load(input);
            loaded = true;
            Logger.info("Configuration loaded from {}", CONFIG_FILE);
        } catch (IOException e) {
            Logger.warn("Could not load configuration file {}, using defaults", CONFIG_FILE);
            loadDefaults();
        }
    }

    /**
     * Loads default configuration values
     */
    private static void loadDefaults() {
        // Server defaults
        properties.setProperty(SERVER_PORT, "9092");
        properties.setProperty(SERVER_HOST, "0.0.0.0");
        properties.setProperty(THREAD_POOL_SIZE, "10");
        properties.setProperty(MAX_CONNECTIONS, "100");
        
        // Topic defaults
        properties.setProperty(DEFAULT_PARTITIONS, "3");
        properties.setProperty(MAX_TOPIC_NAME_LENGTH, "255");
        properties.setProperty(TOPIC_CLEANUP_INTERVAL, "300000"); // 5 minutes
        
        // Message defaults
        properties.setProperty(MAX_MESSAGE_SIZE, "1048576"); // 1MB
        properties.setProperty(MESSAGE_RETENTION_MS, "604800000"); // 7 days
        properties.setProperty(ENABLE_COMPRESSION, "true");
        properties.setProperty(COMPRESSION_THRESHOLD, "1024"); // 1KB
        
        // Network defaults
        properties.setProperty(SOCKET_TIMEOUT_MS, "30000"); // 30 seconds
        properties.setProperty(CONNECTION_TIMEOUT_MS, "10000"); // 10 seconds
        properties.setProperty(RECEIVE_BUFFER_SIZE, "65536"); // 64KB
        properties.setProperty(SEND_BUFFER_SIZE, "65536"); // 64KB
        
        // Logging defaults
        properties.setProperty(LOG_LEVEL, "INFO");
        properties.setProperty(LOG_FILE_MAX_SIZE, "100");
        properties.setProperty(LOG_FILE_MAX_HISTORY, "30");
        
        // Performance defaults
        properties.setProperty(BATCH_SIZE, "1000");
        properties.setProperty(BATCH_TIMEOUT_MS, "1000"); // 1 second
        properties.setProperty(ENABLE_METRICS, "true");
        properties.setProperty(METRICS_INTERVAL_MS, "60000"); // 1 minute
        
        loaded = true;
        Logger.info("Using default configuration values");
    }

    /**
     * Gets a string configuration value
     */
    public static String getString(String key) {
        return properties.getProperty(key);
    }

    /**
     * Gets a string configuration value with default
     */
    public static String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    /**
     * Gets an integer configuration value
     */
    public static int getInt(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    /**
     * Gets an integer configuration value with default
     */
    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    /**
     * Gets a long configuration value
     */
    public static long getLong(String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    /**
     * Gets a long configuration value with default
     */
    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Long.parseLong(value) : defaultValue;
    }

    /**
     * Gets a boolean configuration value
     */
    public static boolean getBoolean(String key) {
        return Boolean.parseBoolean(properties.getProperty(key));
    }

    /**
     * Gets a boolean configuration value with default
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    /**
     * Sets a configuration value
     */
    public static void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    /**
     * Checks if configuration is loaded
     */
    public static boolean isLoaded() {
        return loaded;
    }

    /**
     * Gets all configuration properties
     */
    public static Properties getAllProperties() {
        return new Properties(properties);
    }

    /**
     * Reloads configuration from file
     */
    public static void reload() {
        loaded = false;
        loadConfiguration();
    }

    /**
     * Prints current configuration
     */
    public static void printConfiguration() {
        Logger.info("Current Kafka Configuration:");
        properties.forEach((key, value) -> 
            Logger.info("  {} = {}", key, value));
    }
}
