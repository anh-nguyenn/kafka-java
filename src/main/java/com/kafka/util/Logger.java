package com.kafka.util;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Centralized logging utility for the Kafka-like broker.
 * Provides consistent logging across all components.
 */
public class Logger {
    private static final Logger logger = LoggerFactory.getLogger("KafkaBroker");
    
    public static void info(String message) {
        logger.info(message);
    }
    
    public static void info(String format, Object... args) {
        logger.info(format, args);
    }
    
    public static void warn(String message) {
        logger.warn(message);
    }
    
    public static void warn(String format, Object... args) {
        logger.warn(format, args);
    }
    
    public static void error(String message) {
        logger.error(message);
    }
    
    public static void error(String message, Throwable throwable) {
        logger.error(message, throwable);
    }
    
    public static void error(String format, Object... args) {
        logger.error(format, args);
    }
    
    public static void debug(String message) {
        logger.debug(message);
    }
    
    public static void debug(String format, Object... args) {
        logger.debug(format, args);
    }
}
