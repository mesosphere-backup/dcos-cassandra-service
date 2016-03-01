package com.mesosphere.dcos.cassandra.executor.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface CompressionDriver {
    /**
     * Compresses the file/directory denoted by {@code sourcePath}, and produces an archive at {@code destinationPath}.
     *
     * @param sourcePath      Source path to compress.
     * @param destinationPath Destination path.
     */
    void compress(String sourcePath, String destinationPath) throws IOException;

    void decompress(InputStream source, OutputStream destination) throws IOException;
}
