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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * CompressionDriver is the interface to all Compression implementations that
 * compress and decompress snapshots during a Cassandra backup.
 */
public interface CompressionDriver {
    /**
     * Compresses the file/directory denoted by {@code sourcePath}, and produces an archive at {@code destinationPath}.
     *
     * @param sourcePath      Source path to compress.
     * @param destinationPath Destination path.
     */
    void compress(String sourcePath, String destinationPath) throws IOException;

    /**
     * Decompresses the file/directory from source and stores it at destination.
     * @param source The InputStream containing the source data to compress.
     * @param destination The OutputStream containing the destination to
     *                    write the decompressed output.
     * @throws IOException If decompression fails.
     */
    void decompress(InputStream source, OutputStream destination)
            throws IOException;
}
