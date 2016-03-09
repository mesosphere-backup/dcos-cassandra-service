package com.mesosphere.dcos.cassandra.executor.compress;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

public class SnappyCompressionDriver implements CompressionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnappyCompressionDriver.class);

    public static final int DEFAULT_BUFFER_SIZE = 4 * 1024; // 3KB

    public void compress(final InputStream source, final OutputStream destination) {

    }

    @Override
    public void compress(final String sourcePath, final String destinationPath) throws IOException {
        BufferedInputStream inputStream = null;
        SnappyOutputStream compressedStream = null;
        try {
            inputStream = new BufferedInputStream(new FileInputStream(sourcePath));
            compressedStream = new SnappyOutputStream(
                    new BufferedOutputStream(new FileOutputStream(destinationPath)));
            IOUtils.copy(inputStream, compressedStream);
        } catch (IOException e) {
            LOGGER.error("Failed to compress {} to {} due to: {}", sourcePath, destinationPath, e);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(compressedStream);
        }
    }

    @Override
    public void decompress(InputStream source, OutputStream destination) throws IOException {
        SnappyInputStream compressedInputStream = null;
        BufferedOutputStream output = null;
        try {
            compressedInputStream = new SnappyInputStream(new BufferedInputStream(source));
            output = new BufferedOutputStream(destination, DEFAULT_BUFFER_SIZE);
            
            int bytesRead;
            byte[] data = new byte[DEFAULT_BUFFER_SIZE];
            while ((bytesRead = compressedInputStream.read(data, 0, DEFAULT_BUFFER_SIZE)) != -1) {
                output.write(data, 0, bytesRead);
            }
        } finally {
            IOUtils.closeQuietly(compressedInputStream);
            IOUtils.closeQuietly(output);
        }
    }
}
