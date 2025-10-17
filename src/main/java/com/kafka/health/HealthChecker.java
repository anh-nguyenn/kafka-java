package com.kafka.health;

import com.kafka.metrics.MetricsCollector;
import com.kafka.util.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Health check system for monitoring broker status and system resources.
 * Provides comprehensive health information for monitoring and alerting.
 */
public class HealthChecker {
    private static final HealthChecker INSTANCE = new HealthChecker();
    
    private final AtomicBoolean isHealthy = new AtomicBoolean(true);
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    private final MetricsCollector metricsCollector = MetricsCollector.getInstance();
    
    // Health thresholds
    private static final long MAX_MEMORY_USAGE_PERCENT = 90;
    private static final long MAX_GC_TIME_PERCENT = 10;
    private static final long MAX_ERROR_RATE_PERCENT = 5;

    private HealthChecker() {}

    public static HealthChecker getInstance() {
        return INSTANCE;
    }

    /**
     * Performs a comprehensive health check
     */
    public HealthStatus checkHealth() {
        HealthStatus status = new HealthStatus();
        
        // Check memory usage
        checkMemoryHealth(status);
        
        // Check GC performance
        checkGCHealth(status);
        
        // Check error rates
        checkErrorRates(status);
        
        // Check uptime
        checkUptime(status);
        
        // Overall health determination
        isHealthy.set(status.isHealthy());
        
        return status;
    }

    /**
     * Checks memory usage health
     */
    private void checkMemoryHealth(HealthStatus status) {
        long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
        long maxMemory = memoryBean.getHeapMemoryUsage().getMax();
        long memoryUsagePercent = (usedMemory * 100) / maxMemory;
        
        status.setMemoryUsagePercent(memoryUsagePercent);
        status.setUsedMemoryMB(usedMemory / (1024 * 1024));
        status.setMaxMemoryMB(maxMemory / (1024 * 1024));
        
        if (memoryUsagePercent > MAX_MEMORY_USAGE_PERCENT) {
            status.addIssue("High memory usage: " + memoryUsagePercent + "%");
            status.setHealthy(false);
        }
    }

    /**
     * Checks garbage collection health
     */
    private void checkGCHealth(HealthStatus status) {
        long totalGcTime = 0;
        for (var gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            totalGcTime += gcBean.getCollectionTime();
        }
        
        long uptime = runtimeBean.getUptime();
        long gcTimePercent = uptime > 0 ? (totalGcTime * 100) / uptime : 0;
        
        status.setGcTimePercent(gcTimePercent);
        status.setTotalGcTimeMs(totalGcTime);
        
        if (gcTimePercent > MAX_GC_TIME_PERCENT) {
            status.addIssue("High GC time: " + gcTimePercent + "%");
            status.setHealthy(false);
        }
    }

    /**
     * Checks error rates
     */
    private void checkErrorRates(HealthStatus status) {
        var metrics = metricsCollector.getSnapshot();
        long totalRequests = metrics.getRequestsProcessed();
        long errors = metrics.getErrorsCount();
        
        double errorRate = totalRequests > 0 ? (errors * 100.0) / totalRequests : 0.0;
        status.setErrorRatePercent(errorRate);
        status.setTotalRequests(totalRequests);
        status.setTotalErrors(errors);
        
        if (errorRate > MAX_ERROR_RATE_PERCENT) {
            status.addIssue("High error rate: " + String.format("%.2f", errorRate) + "%");
            status.setHealthy(false);
        }
    }

    /**
     * Checks uptime and system stability
     */
    private void checkUptime(HealthStatus status) {
        long uptime = runtimeBean.getUptime();
        status.setUptimeMs(uptime);
        
        // Consider unhealthy if uptime is less than 1 minute (likely restart)
        if (uptime < 60000) {
            status.addIssue("Recent restart detected");
            status.setHealthy(false);
        }
    }

    /**
     * Gets a quick health status
     */
    public boolean isHealthy() {
        return isHealthy.get();
    }

    /**
     * Gets detailed health information as JSON
     */
    public String getHealthJson() {
        HealthStatus status = checkHealth();
        
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"status\": \"").append(status.isHealthy() ? "healthy" : "unhealthy").append("\",\n");
        json.append("  \"timestamp\": ").append(System.currentTimeMillis()).append(",\n");
        json.append("  \"uptime_ms\": ").append(status.getUptimeMs()).append(",\n");
        json.append("  \"memory\": {\n");
        json.append("    \"used_mb\": ").append(status.getUsedMemoryMB()).append(",\n");
        json.append("    \"max_mb\": ").append(status.getMaxMemoryMB()).append(",\n");
        json.append("    \"usage_percent\": ").append(status.getMemoryUsagePercent()).append("\n");
        json.append("  },\n");
        json.append("  \"gc\": {\n");
        json.append("    \"time_percent\": ").append(status.getGcTimePercent()).append(",\n");
        json.append("    \"total_time_ms\": ").append(status.getTotalGcTimeMs()).append("\n");
        json.append("  },\n");
        json.append("  \"requests\": {\n");
        json.append("    \"total\": ").append(status.getTotalRequests()).append(",\n");
        json.append("    \"errors\": ").append(status.getTotalErrors()).append(",\n");
        json.append("    \"error_rate_percent\": ").append(String.format("%.2f", status.getErrorRatePercent())).append("\n");
        json.append("  },\n");
        json.append("  \"issues\": [");
        
        if (status.getIssues().isEmpty()) {
            json.append("]");
        } else {
            for (int i = 0; i < status.getIssues().size(); i++) {
                if (i > 0) json.append(", ");
                json.append("\"").append(status.getIssues().get(i)).append("\"");
            }
            json.append("]");
        }
        
        json.append("\n}");
        
        return json.toString();
    }

    /**
     * Health status data class
     */
    public static class HealthStatus {
        private boolean healthy = true;
        private long uptimeMs;
        private long usedMemoryMB;
        private long maxMemoryMB;
        private long memoryUsagePercent;
        private long gcTimePercent;
        private long totalGcTimeMs;
        private long totalRequests;
        private long totalErrors;
        private double errorRatePercent;
        private java.util.List<String> issues = new java.util.ArrayList<>();

        // Getters and setters
        public boolean isHealthy() { return healthy; }
        public void setHealthy(boolean healthy) { this.healthy = healthy; }
        
        public long getUptimeMs() { return uptimeMs; }
        public void setUptimeMs(long uptimeMs) { this.uptimeMs = uptimeMs; }
        
        public long getUsedMemoryMB() { return usedMemoryMB; }
        public void setUsedMemoryMB(long usedMemoryMB) { this.usedMemoryMB = usedMemoryMB; }
        
        public long getMaxMemoryMB() { return maxMemoryMB; }
        public void setMaxMemoryMB(long maxMemoryMB) { this.maxMemoryMB = maxMemoryMB; }
        
        public long getMemoryUsagePercent() { return memoryUsagePercent; }
        public void setMemoryUsagePercent(long memoryUsagePercent) { this.memoryUsagePercent = memoryUsagePercent; }
        
        public long getGcTimePercent() { return gcTimePercent; }
        public void setGcTimePercent(long gcTimePercent) { this.gcTimePercent = gcTimePercent; }
        
        public long getTotalGcTimeMs() { return totalGcTimeMs; }
        public void setTotalGcTimeMs(long totalGcTimeMs) { this.totalGcTimeMs = totalGcTimeMs; }
        
        public long getTotalRequests() { return totalRequests; }
        public void setTotalRequests(long totalRequests) { this.totalRequests = totalRequests; }
        
        public long getTotalErrors() { return totalErrors; }
        public void setTotalErrors(long totalErrors) { this.totalErrors = totalErrors; }
        
        public double getErrorRatePercent() { return errorRatePercent; }
        public void setErrorRatePercent(double errorRatePercent) { this.errorRatePercent = errorRatePercent; }
        
        public java.util.List<String> getIssues() { return issues; }
        public void addIssue(String issue) { this.issues.add(issue); }
    }
}
