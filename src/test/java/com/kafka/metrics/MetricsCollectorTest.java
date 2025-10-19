package com.kafka.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for MetricsCollector
 */
public class MetricsCollectorTest {

    private MetricsCollector metricsCollector;

    @BeforeEach
    public void setUp() {
        metricsCollector = MetricsCollector.getInstance();
        metricsCollector.reset();
    }

    @Test
    public void testRecordMessageProduced() {
        metricsCollector.recordMessageProduced("test-topic", 0, 100, 50);

        var snapshot = metricsCollector.getSnapshot();
        assertEquals(1, snapshot.getMessagesProduced());
        assertEquals(100, snapshot.getBytesProduced());
        assertEquals(50.0, snapshot.getAverageProduceLatency(), 0.001);
    }

    @Test
    public void testRecordMessageConsumed() {
        metricsCollector.recordMessageConsumed("test-topic", 0, 200, 30);

        var snapshot = metricsCollector.getSnapshot();
        assertEquals(1, snapshot.getMessagesConsumed());
        assertEquals(200, snapshot.getBytesConsumed());
        assertEquals(30.0, snapshot.getAverageConsumeLatency(), 0.001);
    }

    @Test
    public void testRecordRequest() {
        metricsCollector.recordRequest("PRODUCE", 25);

        var snapshot = metricsCollector.getSnapshot();
        assertEquals(1, snapshot.getRequestsProcessed());
        assertEquals(25.0, snapshot.getAverageRequestLatency(), 0.001);
    }

    @Test
    public void testRecordError() {
        metricsCollector.recordError("TIMEOUT");

        var snapshot = metricsCollector.getSnapshot();
        assertEquals(1, snapshot.getErrorsCount());
    }

    @Test
    public void testMultipleRecords() {
        // Record multiple messages
        metricsCollector.recordMessageProduced("topic1", 0, 100, 50);
        metricsCollector.recordMessageProduced("topic1", 1, 200, 60);
        metricsCollector.recordMessageConsumed("topic1", 0, 150, 40);

        // Record requests
        metricsCollector.recordRequest("PRODUCE", 25);
        metricsCollector.recordRequest("FETCH", 30);

        // Record errors
        metricsCollector.recordError("TIMEOUT");
        metricsCollector.recordError("CONNECTION_LOST");

        var snapshot = metricsCollector.getSnapshot();
        assertEquals(2, snapshot.getMessagesProduced());
        assertEquals(1, snapshot.getMessagesConsumed());
        assertEquals(300, snapshot.getBytesProduced());
        assertEquals(150, snapshot.getBytesConsumed());
        assertEquals(2, snapshot.getRequestsProcessed());
        assertEquals(2, snapshot.getErrorsCount());
        assertEquals(55.0, snapshot.getAverageProduceLatency(), 0.001);
        assertEquals(40.0, snapshot.getAverageConsumeLatency(), 0.001);
        assertEquals(27.5, snapshot.getAverageRequestLatency(), 0.001);
    }

    @Test
    public void testReset() {
        // Record some metrics
        metricsCollector.recordMessageProduced("test-topic", 0, 100, 50);
        metricsCollector.recordRequest("PRODUCE", 25);
        metricsCollector.recordError("TIMEOUT");

        // Reset
        metricsCollector.reset();

        var snapshot = metricsCollector.getSnapshot();
        assertEquals(0, snapshot.getMessagesProduced());
        assertEquals(0, snapshot.getMessagesConsumed());
        assertEquals(0, snapshot.getBytesProduced());
        assertEquals(0, snapshot.getBytesConsumed());
        assertEquals(0, snapshot.getRequestsProcessed());
        assertEquals(0, snapshot.getErrorsCount());
        assertEquals(0.0, snapshot.getAverageProduceLatency(), 0.001);
        assertEquals(0.0, snapshot.getAverageConsumeLatency(), 0.001);
        assertEquals(0.0, snapshot.getAverageRequestLatency(), 0.001);
    }

    @Test
    public void testTopicMetrics() {
        metricsCollector.recordMessageProduced("topic1", 0, 100, 50);
        metricsCollector.recordMessageProduced("topic1", 1, 200, 60);
        metricsCollector.recordMessageConsumed("topic1", 0, 150, 40);

        metricsCollector.recordMessageProduced("topic2", 0, 300, 70);

        var snapshot = metricsCollector.getSnapshot();
        var topicMetrics = snapshot.getTopicMetrics();

        assertTrue(topicMetrics.containsKey("topic1"));
        assertTrue(topicMetrics.containsKey("topic2"));

        var topic1Metrics = topicMetrics.get("topic1");
        assertEquals(2, topic1Metrics.getMessagesProduced());
        assertEquals(1, topic1Metrics.getMessagesConsumed());
        assertEquals(300, topic1Metrics.getBytesProduced());
        assertEquals(150, topic1Metrics.getBytesConsumed());

        var topic2Metrics = topicMetrics.get("topic2");
        assertEquals(1, topic2Metrics.getMessagesProduced());
        assertEquals(0, topic2Metrics.getMessagesConsumed());
        assertEquals(300, topic2Metrics.getBytesProduced());
        assertEquals(0, topic2Metrics.getBytesConsumed());
    }

    @Test
    public void testRequestCounts() {
        metricsCollector.recordRequest("PRODUCE", 25);
        metricsCollector.recordRequest("PRODUCE", 30);
        metricsCollector.recordRequest("FETCH", 20);

        var snapshot = metricsCollector.getSnapshot();
        var requestCounts = snapshot.getRequestCounts();

        assertEquals(2, requestCounts.get("PRODUCE").sum());
        assertEquals(1, requestCounts.get("FETCH").sum());
    }

    @Test
    public void testUptime() {
        var snapshot = metricsCollector.getSnapshot();
        assertTrue(snapshot.getUptime() > 0);
        assertTrue(snapshot.getTimeSinceReset() > 0);
    }
}

