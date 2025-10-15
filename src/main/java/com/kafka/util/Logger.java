package com.kafka.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Centralized logging utility for the Kafka-like broker.
 * Provides consistent logging across all components.
 */
public class Logger {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    public static void info(String message) {
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] [INFO] KafkaBroker - " + message);
    }
    
    public static void info(String format, Object... args) {
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] [INFO] KafkaBroker - " + String.format(format, args));
    }
    
    public static void warn(String message) {
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] [WARN] KafkaBroker - " + message);
    }
    
    public static void warn(String format, Object... args) {
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] [WARN] KafkaBroker - " + String.format(format, args));
    }
    
    public static void error(String message) {
        System.err.println("[" + LocalDateTime.now().format(formatter) + "] [ERROR] KafkaBroker - " + message);
    }
    
    public static void error(String message, Throwable throwable) {
        System.err.println("[" + LocalDateTime.now().format(formatter) + "] [ERROR] KafkaBroker - " + message);
        throwable.printStackTrace();
    }
    
    public static void error(String format, Object... args) {
        System.err.println("[" + LocalDateTime.now().format(formatter) + "] [ERROR] KafkaBroker - " + String.format(format, args));
    }
    
    public static void debug(String message) {
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] [DEBUG] KafkaBroker - " + message);
    }
    
    public static void debug(String format, Object... args) {
        System.out.println("[" + LocalDateTime.now().format(formatter) + "] [DEBUG] KafkaBroker - " + String.format(format, args));
    }
}
