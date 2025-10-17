package com.kafka.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for KafkaConfig
 */
public class KafkaConfigTest {

    @Test
    public void testConfigurationLoaded() {
        assertTrue(KafkaConfig.isLoaded(), "Configuration should be loaded");
    }

    @Test
    public void testStringConfiguration() {
        String port = KafkaConfig.getString(KafkaConfig.SERVER_PORT);
        assertNotNull(port);
        assertEquals("9092", port);
    }

    @Test
    public void testStringConfigurationWithDefault() {
        String value = KafkaConfig.getString("nonexistent.key", "default");
        assertEquals("default", value);
    }

    @Test
    public void testIntConfiguration() {
        int port = KafkaConfig.getInt(KafkaConfig.SERVER_PORT);
        assertEquals(9092, port);
    }

    @Test
    public void testIntConfigurationWithDefault() {
        int value = KafkaConfig.getInt("nonexistent.key", 42);
        assertEquals(42, value);
    }

    @Test
    public void testLongConfiguration() {
        long retention = KafkaConfig.getLong(KafkaConfig.MESSAGE_RETENTION_MS);
        assertEquals(604800000L, retention);
    }

    @Test
    public void testLongConfigurationWithDefault() {
        long value = KafkaConfig.getLong("nonexistent.key", 123L);
        assertEquals(123L, value);
    }

    @Test
    public void testBooleanConfiguration() {
        boolean compression = KafkaConfig.getBoolean(KafkaConfig.ENABLE_COMPRESSION);
        assertTrue(compression);
    }

    @Test
    public void testBooleanConfigurationWithDefault() {
        boolean value = KafkaConfig.getBoolean("nonexistent.key", true);
        assertTrue(value);
    }

    @Test
    public void testSetProperty() {
        String originalValue = KafkaConfig.getString("test.key", "original");
        KafkaConfig.setProperty("test.key", "modified");
        String newValue = KafkaConfig.getString("test.key");
        assertEquals("modified", newValue);
        
        // Restore original value
        KafkaConfig.setProperty("test.key", originalValue);
    }

    @Test
    public void testGetAllProperties() {
        var properties = KafkaConfig.getAllProperties();
        assertNotNull(properties);
        assertFalse(properties.isEmpty());
        assertTrue(properties.containsKey(KafkaConfig.SERVER_PORT));
    }

    @Test
    public void testConfigurationConstants() {
        // Test that all configuration keys are defined
        assertNotNull(KafkaConfig.SERVER_PORT);
        assertNotNull(KafkaConfig.SERVER_HOST);
        assertNotNull(KafkaConfig.THREAD_POOL_SIZE);
        assertNotNull(KafkaConfig.DEFAULT_PARTITIONS);
        assertNotNull(KafkaConfig.MAX_MESSAGE_SIZE);
        assertNotNull(KafkaConfig.ENABLE_COMPRESSION);
    }
}
