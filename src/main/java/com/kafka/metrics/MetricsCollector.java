package com.kafka.metrics;

import com.kafka.config.KafkaConfig;
import com.kafka.util.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collection system for monitoring Kafka broker performance.
 * Collects various metrics including throughput, latency, and resource usage.
 */
public class MetricsCollector {
    private static final MetricsCollector INSTANCE = new MetricsCollector();
    
    // Counters for various metrics
    private final LongAdder messagesProduced = new LongAdder();
    private final LongAdder messagesConsumed = new LongAdder();
    private final LongAdder bytesProduced = new LongAdder();
    private final LongAdder bytesConsumed = new LongAdder();
    private final LongAdder requestsProcessed = new LongAdder();
    private final LongAdder errorsCount = new LongAdder();
    
    // Latency tracking
    private final AtomicLong totalProduceLatency = new AtomicLong(0);
    private final AtomicLong totalConsumeLatency = new AtomicLong(0);
    private final AtomicLong totalRequestLatency = new AtomicLong(0);
    
    // Request counts by type
    private final ConcurrentHashMap<String, LongAdder> requestCounts = new ConcurrentHashMap<>();
    
    // Topic-specific metrics
    private final ConcurrentHashMap<String, TopicMetrics> topicMetrics = new ConcurrentHashMap<>();
    
    // System metrics
    private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastResetTime = new AtomicLong(System.currentTimeMillis());
    
    private final ScheduledExecutorService scheduler;
    private final boolean metricsEnabled;
    private final long metricsIntervalMs;

    private MetricsCollector() {
        this.metricsEnabled = KafkaConfig.getBoolean(KafkaConfig.ENABLE_METRICS, true);
        this.metricsIntervalMs = KafkaConfig.getLong(KafkaConfig.METRICS_INTERVAL_MS, 60000);
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        if (metricsEnabled) {
            startMetricsReporting();
        }
    }

    public static MetricsCollector getInstance() {
        return INSTANCE;
    }

    /**
     * Records a message production
     */
    public void recordMessageProduced(String topic, int partition, int messageSize, long latencyMs) {
        if (!metricsEnabled) return;
        
        messagesProduced.increment();
        bytesProduced.add(messageSize);
        totalProduceLatency.addAndGet(latencyMs);
        
        getTopicMetrics(topic).recordMessageProduced(messageSize, latencyMs);
    }

    /**
     * Records a message consumption
     */
    public void recordMessageConsumed(String topic, int partition, int messageSize, long latencyMs) {
        if (!metricsEnabled) return;
        
        messagesConsumed.increment();
        bytesConsumed.add(messageSize);
        totalConsumeLatency.addAndGet(latencyMs);
        
        getTopicMetrics(topic).recordMessageConsumed(messageSize, latencyMs);
    }

    /**
     * Records a request processing
     */
    public void recordRequest(String requestType, long latencyMs) {
        if (!metricsEnabled) return;
        
        requestsProcessed.increment();
        totalRequestLatency.addAndGet(latencyMs);
        
        requestCounts.computeIfAbsent(requestType, k -> new LongAdder()).increment();
    }

    /**
     * Records an error
     */
    public void recordError(String errorType) {
        if (!metricsEnabled) return;
        
        errorsCount.increment();
    }

    /**
     * Gets current metrics snapshot
     */
    public MetricsSnapshot getSnapshot() {
        long currentTime = System.currentTimeMillis();
        long uptime = currentTime - startTime.get();
        long timeSinceReset = currentTime - lastResetTime.get();
        
        return new MetricsSnapshot(
            messagesProduced.sum(),
            messagesConsumed.sum(),
            bytesProduced.sum(),
            bytesConsumed.sum(),
            requestsProcessed.sum(),
            errorsCount.sum(),
            getAverageLatency(totalProduceLatency.get(), messagesProduced.sum()),
            getAverageLatency(totalConsumeLatency.get(), messagesConsumed.sum()),
            getAverageLatency(totalRequestLatency.get(), requestsProcessed.sum()),
            uptime,
            timeSinceReset,
            new ConcurrentHashMap<>(requestCounts),
            new ConcurrentHashMap<>(topicMetrics)
        );
    }

    /**
     * Resets all metrics
     */
    public void reset() {
        messagesProduced.reset();
        messagesConsumed.reset();
        bytesProduced.reset();
        bytesConsumed.reset();
        requestsProcessed.reset();
        errorsCount.reset();
        
        totalProduceLatency.set(0);
        totalConsumeLatency.set(0);
        totalRequestLatency.set(0);
        
        requestCounts.clear();
        topicMetrics.clear();
        
        lastResetTime.set(System.currentTimeMillis());
        
        Logger.info("Metrics reset at {}", System.currentTimeMillis());
    }

    /**
     * Starts periodic metrics reporting
     */
    private void startMetricsReporting() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                reportMetrics();
            } catch (Exception e) {
                Logger.error("Error reporting metrics", e);
            }
        }, metricsIntervalMs, metricsIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Reports current metrics
     */
    private void reportMetrics() {
        MetricsSnapshot snapshot = getSnapshot();
        
        Logger.info("=== Kafka Broker Metrics ===");
        Logger.info("Uptime: {} ms", snapshot.getUptime());
        Logger.info("Messages produced: {}", snapshot.getMessagesProduced());
        Logger.info("Messages consumed: {}", snapshot.getMessagesConsumed());
        Logger.info("Bytes produced: {} MB", snapshot.getBytesProduced() / (1024 * 1024));
        Logger.info("Bytes consumed: {} MB", snapshot.getBytesConsumed() / (1024 * 1024));
        Logger.info("Requests processed: {}", snapshot.getRequestsProcessed());
        Logger.info("Errors: {}", snapshot.getErrorsCount());
        Logger.info("Avg produce latency: {} ms", snapshot.getAverageProduceLatency());
        Logger.info("Avg consume latency: {} ms", snapshot.getAverageConsumeLatency());
        Logger.info("Avg request latency: {} ms", snapshot.getAverageRequestLatency());
        
        // Report per-topic metrics
        snapshot.getTopicMetrics().forEach((topic, metrics) -> {
            Logger.info("Topic '{}': produced={}, consumed={}, avg_latency={}ms", 
                       topic, metrics.getMessagesProduced(), metrics.getMessagesConsumed(), 
                       metrics.getAverageLatency());
        });
        
        Logger.info("==========================");
    }

    /**
     * Gets topic-specific metrics
     */
    private TopicMetrics getTopicMetrics(String topic) {
        return topicMetrics.computeIfAbsent(topic, k -> new TopicMetrics());
    }

    /**
     * Calculates average latency
     */
    private double getAverageLatency(long totalLatency, long count) {
        return count > 0 ? (double) totalLatency / count : 0.0;
    }

    /**
     * Shuts down the metrics collector
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Topic-specific metrics
     */
    public static class TopicMetrics {
        private final LongAdder messagesProduced = new LongAdder();
        private final LongAdder messagesConsumed = new LongAdder();
        private final LongAdder bytesProduced = new LongAdder();
        private final LongAdder bytesConsumed = new LongAdder();
        private final AtomicLong totalLatency = new AtomicLong(0);

        public void recordMessageProduced(int messageSize, long latencyMs) {
            messagesProduced.increment();
            bytesProduced.add(messageSize);
            totalLatency.addAndGet(latencyMs);
        }

        public void recordMessageConsumed(int messageSize, long latencyMs) {
            messagesConsumed.increment();
            bytesConsumed.add(messageSize);
            totalLatency.addAndGet(latencyMs);
        }

        public long getMessagesProduced() { return messagesProduced.sum(); }
        public long getMessagesConsumed() { return messagesConsumed.sum(); }
        public long getBytesProduced() { return bytesProduced.sum(); }
        public long getBytesConsumed() { return bytesConsumed.sum(); }
        
        public double getAverageLatency() {
            long totalMessages = messagesProduced.sum() + messagesConsumed.sum();
            return totalMessages > 0 ? (double) totalLatency.get() / totalMessages : 0.0;
        }
    }

    /**
     * Metrics snapshot for reporting
     */
    public static class MetricsSnapshot {
        private final long messagesProduced;
        private final long messagesConsumed;
        private final long bytesProduced;
        private final long bytesConsumed;
        private final long requestsProcessed;
        private final long errorsCount;
        private final double averageProduceLatency;
        private final double averageConsumeLatency;
        private final double averageRequestLatency;
        private final long uptime;
        private final long timeSinceReset;
        private final ConcurrentHashMap<String, LongAdder> requestCounts;
        private final ConcurrentHashMap<String, TopicMetrics> topicMetrics;

        public MetricsSnapshot(long messagesProduced, long messagesConsumed, long bytesProduced, 
                             long bytesConsumed, long requestsProcessed, long errorsCount,
                             double averageProduceLatency, double averageConsumeLatency, 
                             double averageRequestLatency, long uptime, long timeSinceReset,
                             ConcurrentHashMap<String, LongAdder> requestCounts,
                             ConcurrentHashMap<String, TopicMetrics> topicMetrics) {
            this.messagesProduced = messagesProduced;
            this.messagesConsumed = messagesConsumed;
            this.bytesProduced = bytesProduced;
            this.bytesConsumed = bytesConsumed;
            this.requestsProcessed = requestsProcessed;
            this.errorsCount = errorsCount;
            this.averageProduceLatency = averageProduceLatency;
            this.averageConsumeLatency = averageConsumeLatency;
            this.averageRequestLatency = averageRequestLatency;
            this.uptime = uptime;
            this.timeSinceReset = timeSinceReset;
            this.requestCounts = requestCounts;
            this.topicMetrics = topicMetrics;
        }

        // Getters
        public long getMessagesProduced() { return messagesProduced; }
        public long getMessagesConsumed() { return messagesConsumed; }
        public long getBytesProduced() { return bytesProduced; }
        public long getBytesConsumed() { return bytesConsumed; }
        public long getRequestsProcessed() { return requestsProcessed; }
        public long getErrorsCount() { return errorsCount; }
        public double getAverageProduceLatency() { return averageProduceLatency; }
        public double getAverageConsumeLatency() { return averageConsumeLatency; }
        public double getAverageRequestLatency() { return averageRequestLatency; }
        public long getUptime() { return uptime; }
        public long getTimeSinceReset() { return timeSinceReset; }
        public ConcurrentHashMap<String, LongAdder> getRequestCounts() { return requestCounts; }
        public ConcurrentHashMap<String, TopicMetrics> getTopicMetrics() { return topicMetrics; }
    }
}
