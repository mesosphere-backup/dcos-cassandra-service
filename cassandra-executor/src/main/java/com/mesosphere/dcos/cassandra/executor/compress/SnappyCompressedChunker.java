package com.mesosphere.dcos.cassandra.executor.compress;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * SnappyCompressedChunker reads bytes from InputStream, compress them, and return a stream of byte[]. The size of
 * byte[] is equal to compressedChunkSize, for all but the last chunk.
 */
public class SnappyCompressedChunker implements Iterator<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnappyCompressedChunker.class);

    /**
     * Number of bytes to read from disk.
     */
    private static int READ_BUFFER_SIZE = 4 * 1024 * 1024; // 4MB

    /**
     * Amount of compressed bytes to buffer.
     */
    private int compressedChunkSize;

    private boolean hasNextChunk;
    private ByteArrayOutputStream baos;
    private InputStream fileStreamToCompress;
    private SnappyOutputStream compressedStream;

    public SnappyCompressedChunker(final InputStream fileToCompress,
                                   final int compressedChunkSize) throws IOException {
        this.fileStreamToCompress = fileToCompress;
        this.compressedChunkSize = compressedChunkSize;
        this.hasNextChunk = true;

        // Initialize Snappy compression stream
        this.baos = new ByteArrayOutputStream();
        this.compressedStream = new SnappyOutputStream(baos);
    }

    @Override
    public boolean hasNext() {
        return hasNextChunk;
    }

    @Override
    public byte[] next() {
        byte[] readBuffer = new byte[READ_BUFFER_SIZE];
        boolean eof = false;
        int bytesRead = 0;
        try {
            while (baos.size() < compressedChunkSize) {
                bytesRead = fileStreamToCompress.read(readBuffer);
                if (bytesRead != -1) {
                    compressedStream.write(readBuffer, 0, bytesRead);
                } else {
                    eof = true;
                    break;
                }
            }

            return spillExcessAndReturn(eof);
        } catch (IOException e) {
            if (bytesRead == -1) {
                try {
                    return spillExcessAndReturn(true);
                } catch (IOException ex) {
                    LOGGER.error(ex.getMessage(), ex);
                    throw new RuntimeException(ex);
                }
            } else {
                LOGGER.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private byte[] spillExcessAndReturn(boolean eof) throws IOException {
        compressedStream.flush();
        byte[] compressedBytes = baos.toByteArray();
        byte[] bytesToReturn;

        if (eof) {
            // Reached EOF, return everything.
            hasNextChunk = false;
            bytesToReturn = compressedBytes;
            IOUtils.closeQuietly(fileStreamToCompress);
            IOUtils.closeQuietly(baos);
            IOUtils.closeQuietly(compressedStream);
        } else {
            // Only return compressedChunkSize, and save the spill for next.
            bytesToReturn = Arrays.copyOfRange(compressedBytes, 0, compressedChunkSize);
            byte[] spilledBytes = Arrays.copyOfRange(compressedBytes, compressedChunkSize, compressedBytes.length);
            baos.reset();
            baos.write(spilledBytes);
        }

        return bytesToReturn;
    }

    @Override
    public void remove() {
        // NO-OP
    }
}
