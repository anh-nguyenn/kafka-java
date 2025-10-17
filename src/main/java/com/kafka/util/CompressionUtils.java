package com.kafka.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for message compression and decompression.
 * Supports GZIP compression for reducing network bandwidth usage.
 */
public class CompressionUtils {
    
    /**
     * Compresses data using GZIP compression
     * 
     * @param data The data to compress
     * @return Compressed data
     * @throws IOException if compression fails
     */
    public static byte[] compress(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return data;
        }
        
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            
            gzipOut.write(data);
            gzipOut.finish();
            return baos.toByteArray();
        }
    }
    
    /**
     * Decompresses data using GZIP decompression
     * 
     * @param compressedData The compressed data
     * @return Decompressed data
     * @throws IOException if decompression fails
     */
    public static byte[] decompress(byte[] compressedData) throws IOException {
        if (compressedData == null || compressedData.length == 0) {
            return compressedData;
        }
        
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
             GZIPInputStream gzipIn = new GZIPInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIn.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            
            return baos.toByteArray();
        }
    }
    
    /**
     * Checks if data is compressed by checking for GZIP magic bytes
     * 
     * @param data The data to check
     * @return true if data appears to be GZIP compressed
     */
    public static boolean isCompressed(byte[] data) {
        if (data == null || data.length < 2) {
            return false;
        }
        
        // Check for GZIP magic bytes (0x1f, 0x8b)
        return (data[0] & 0xFF) == 0x1f && (data[1] & 0xFF) == 0x8b;
    }
    
    /**
     * Calculates compression ratio
     * 
     * @param originalSize Original data size
     * @param compressedSize Compressed data size
     * @return Compression ratio (0.0 to 1.0, where 1.0 means no compression)
     */
    public static double getCompressionRatio(int originalSize, int compressedSize) {
        if (originalSize == 0) {
            return 1.0;
        }
        return (double) compressedSize / originalSize;
    }
}
