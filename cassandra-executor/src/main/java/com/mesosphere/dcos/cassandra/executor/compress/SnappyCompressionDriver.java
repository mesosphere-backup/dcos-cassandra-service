/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.executor.compress;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * SnappyCompressionDriver implements CompressionDriver to implement Snappy
 * compression for Cassandra Backup and Restore.
 */
public class SnappyCompressionDriver implements CompressionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SnappyCompressionDriver.class);

    public static final int DEFAULT_BUFFER_SIZE = 4 * 1024; // 3KB

    public void compress(final InputStream source,
                         final OutputStream destination) {

    }

    @Override
    public void compress(final String sourcePath, final String destinationPath)
            throws IOException {
        BufferedInputStream inputStream = null;
        SnappyOutputStream compressedStream = null;
        try {
            inputStream = new BufferedInputStream(
                    new FileInputStream(sourcePath));
            compressedStream = new SnappyOutputStream(
                    new BufferedOutputStream(
                            new FileOutputStream(destinationPath)));
            IOUtils.copy(inputStream, compressedStream);
        } catch (IOException e) {
            LOGGER.error("Failed to compress {} to {} due to: {}", sourcePath,
                    destinationPath, e);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(compressedStream);
        }
    }

    @Override
    public void decompress(InputStream source, OutputStream destination)
            throws IOException {
        SnappyInputStream compressedInputStream = null;
        BufferedOutputStream output = null;
        try {
            compressedInputStream = new SnappyInputStream(
                    new BufferedInputStream(source));
            output = new BufferedOutputStream(destination, DEFAULT_BUFFER_SIZE);

            int bytesRead;
            byte[] data = new byte[DEFAULT_BUFFER_SIZE];
            while ((bytesRead = compressedInputStream.read(data, 0,
                    DEFAULT_BUFFER_SIZE)) != -1) {
                output.write(data, 0, bytesRead);
            }
        } finally {
            IOUtils.closeQuietly(compressedInputStream);
            IOUtils.closeQuietly(output);
        }
    }
}
