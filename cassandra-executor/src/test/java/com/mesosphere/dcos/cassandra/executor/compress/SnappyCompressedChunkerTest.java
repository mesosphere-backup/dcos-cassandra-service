package com.mesosphere.dcos.cassandra.executor.compress;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public class SnappyCompressedChunkerTest {
    private static final String CHAR_POOL = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private static int MB = 1024 * 1024; // 1MB

    private static String generateRandomString(int len) {
        final SecureRandom secureRandom = new SecureRandom();
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            final int nextRandomChar = secureRandom.nextInt(CHAR_POOL.length());
            sb.append(CHAR_POOL.charAt(nextRandomChar));
        }
        return sb.toString();
    }

    @Test
    public void testEightMB() throws IOException {
        final String randomString = generateRandomString(8 * MB);
        final InputStream stream = new ByteArrayInputStream(randomString.getBytes(StandardCharsets.UTF_8));
        final SnappyCompressedChunker snappyCompressedChunker = new SnappyCompressedChunker(stream, 5 * MB);
        Assert.assertTrue(snappyCompressedChunker.hasNext());
        byte[] next = snappyCompressedChunker.next();
        // Compressed version must be equal to 5MB
        Assert.assertEquals(5 * MB, next.length);
        // Should have one more chunk
        Assert.assertTrue(snappyCompressedChunker.hasNext());
        next = snappyCompressedChunker.next();
        // Compressed version must be less than 5MB
        Assert.assertTrue("compressed size should be <= 5242880 but is " + next.length
                , next.length <= 5 * MB);
        // Shouldn't have any more chunks left
        Assert.assertFalse(snappyCompressedChunker.hasNext());
    }

    @Test
    public void testFiveMB() throws IOException {
        final String randomString = generateRandomString(5 * MB);
        final InputStream stream = new ByteArrayInputStream(randomString.getBytes(StandardCharsets.UTF_8));
        final SnappyCompressedChunker snappyCompressedChunker = new SnappyCompressedChunker(stream, 5 * MB);
        Assert.assertTrue(snappyCompressedChunker.hasNext());
        byte[] next = snappyCompressedChunker.next();
        // Compressed version must be equal to 5MB
        Assert.assertTrue("compressed size should be == 5242880 but is " + next.length
                , next.length == 5 * MB);
        // Should have one more chunk left
        Assert.assertTrue(snappyCompressedChunker.hasNext());
        next = snappyCompressedChunker.next();
        // Compressed version must be less than 5MB
        Assert.assertTrue("compressed size should be <= 5242880 but is " + next.length
                , next.length <= 5 * MB);
        Assert.assertFalse(snappyCompressedChunker.hasNext());
    }

    @Test
    public void testFourMB() throws IOException {
        final String randomString = generateRandomString(4 * MB);
        final InputStream stream = new ByteArrayInputStream(randomString.getBytes(StandardCharsets.UTF_8));
        final SnappyCompressedChunker snappyCompressedChunker = new SnappyCompressedChunker(stream, 5 * MB);
        Assert.assertTrue(snappyCompressedChunker.hasNext());
        byte[] next = snappyCompressedChunker.next();
        // Compressed version must be less than 5MB
        Assert.assertTrue("compressed size should be <= 5242880 but is " + next.length
                , next.length < 5 * MB);
        Assert.assertFalse(snappyCompressedChunker.hasNext());
    }

    @Test
    public void testZeroMB() throws IOException {
        final String randomString = generateRandomString(0 * MB);
        final InputStream stream = new ByteArrayInputStream(randomString.getBytes(StandardCharsets.UTF_8));
        final SnappyCompressedChunker snappyCompressedChunker = new SnappyCompressedChunker(stream, 5 * MB);
        Assert.assertTrue(snappyCompressedChunker.hasNext());
        snappyCompressedChunker.next();
        Assert.assertFalse(snappyCompressedChunker.hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInputStream() throws IOException {
        final SnappyCompressedChunker snappyCompressedChunker = new SnappyCompressedChunker(null, 5 * MB);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeChunkSize() throws IOException {
        final String randomString = generateRandomString(0 * MB);
        final InputStream stream = new ByteArrayInputStream(randomString.getBytes(StandardCharsets.UTF_8));
        new SnappyCompressedChunker(stream, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroChunkSize() throws IOException {
        final String randomString = generateRandomString(0 * MB);
        final InputStream stream = new ByteArrayInputStream(randomString.getBytes(StandardCharsets.UTF_8));
        new SnappyCompressedChunker(stream, 0);
    }
}
