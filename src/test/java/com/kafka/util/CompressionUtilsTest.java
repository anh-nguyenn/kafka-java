package com.kafka.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

/**
 * Test cases for CompressionUtils
 */
public class CompressionUtilsTest {

    @Test
    public void testCompressDecompress() throws IOException {
        String originalText = "This is a test message that should compress well because it has repeated patterns. " +
                            "This is a test message that should compress well because it has repeated patterns. " +
                            "This is a test message that should compress well because it has repeated patterns.";
        
        byte[] originalData = originalText.getBytes();
        byte[] compressedData = CompressionUtils.compress(originalData);
        byte[] decompressedData = CompressionUtils.decompress(compressedData);
        
        assertNotNull(compressedData);
        assertNotNull(decompressedData);
        assertArrayEquals(originalData, decompressedData);
        assertTrue(compressedData.length < originalData.length, "Compressed data should be smaller");
    }

    @Test
    public void testCompressEmptyData() throws IOException {
        byte[] emptyData = new byte[0];
        byte[] compressedData = CompressionUtils.compress(emptyData);
        byte[] decompressedData = CompressionUtils.decompress(compressedData);
        
        assertArrayEquals(emptyData, compressedData);
        assertArrayEquals(emptyData, decompressedData);
    }

    @Test
    public void testCompressNullData() throws IOException {
        byte[] compressedData = CompressionUtils.compress(null);
        byte[] decompressedData = CompressionUtils.decompress(null);
        
        assertNull(compressedData);
        assertNull(decompressedData);
    }

    @Test
    public void testIsCompressed() {
        String text = "Test data";
        byte[] originalData = text.getBytes();
        byte[] compressedData = CompressionUtils.compress(originalData);
        
        assertFalse(CompressionUtils.isCompressed(originalData));
        assertTrue(CompressionUtils.isCompressed(compressedData));
    }

    @Test
    public void testCompressionRatio() {
        int originalSize = 1000;
        int compressedSize = 500;
        
        double ratio = CompressionUtils.getCompressionRatio(originalSize, compressedSize);
        assertEquals(0.5, ratio, 0.001);
        
        // Test edge case
        double ratioZero = CompressionUtils.getCompressionRatio(0, 100);
        assertEquals(1.0, ratioZero, 0.001);
    }

    @Test
    public void testCompressionWithSmallData() throws IOException {
        // Small data might not compress well or might even expand
        String smallText = "Hi";
        byte[] originalData = smallText.getBytes();
        byte[] compressedData = CompressionUtils.compress(originalData);
        byte[] decompressedData = CompressionUtils.decompress(compressedData);
        
        assertArrayEquals(originalData, decompressedData);
        // Small data might not compress, so we just verify it doesn't break
    }
}
