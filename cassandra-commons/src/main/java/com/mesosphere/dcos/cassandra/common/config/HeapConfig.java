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
package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HeapConfig is contains the configuration for the JVM heap for a Cassandra
 * node. The size of the heap is the size in Mb and should be 1/4 of the
 * allocated system memory and no greater than 8 Mb.  The new size is the size
 * of the new generation in Mb. It should be set at 100 Mb per cpu core.
 */
public class HeapConfig {

    /**
     * The default heap configuration for a Cassandra node.
     */
    public static final HeapConfig DEFAULT = HeapConfig.create(2048, 100);

    @JsonProperty("size_mb")
    private final int sizeMb;
    @JsonProperty("new_mb")
    private final int newMb;

    /**
     * Factory method creates a new HeapConfig.
     *
     * @param sizeMb The size of the JVM heap in Mb.
     * @param newMb  The size of the new generation in Mb.
     * @return A HeapConfig constructed from the parameters.
     */
    @JsonCreator
    public static HeapConfig create(
            @JsonProperty("size_mb") final int sizeMb,
            @JsonProperty("new_mb") final int newMb) {
        return new HeapConfig(sizeMb, newMb);
    }

    /**
     * Parses a HeapConfig from a Protocol Buffers format.
     *
     * @param heap The Protocol Buffers format of a HeapConfig.
     * @return The HeapConfig parsed from heap.
     */
    public static HeapConfig parse(CassandraProtos.HeapConfig heap) {
        return create(heap.getSizeMb(), heap.getNewMb());
    }

    /**
     * Parses a HeapConfig from a byte array containing a Protocol Buffers
     * serialized format.
     *
     * @param bytes A byte array containing a Protocol Buffers serialized
     *              format of a HeapConfig.
     * @return A HeapConfig parsed from bytes.
     * @throws IOException If a HeapConfig can not be parsed from bytes.
     */
    public static HeapConfig parse(byte[] bytes) throws IOException {

        return parse(CassandraProtos.HeapConfig.parseFrom(bytes));
    }

    /**
     * Constructs a new HeapConfig
     *
     * @param sizeMb The size of the JVM heap in Mb.
     * @param newMb  The size of the new generation in Mb.
     */
    public HeapConfig(final int sizeMb, final int newMb) {
        this.newMb = newMb;
        this.sizeMb = sizeMb;
    }

    /**
     * Gets the heap size.
     *
     * @return The size of the JVM heap in Mb.
     */
    public int getSizeMb() {
        return sizeMb;
    }

    /**
     * Gets the new generation size.
     *
     * @return The size of the new generation in Mb.
     */
    public int getNewMb() {
        return newMb;
    }

    /**
     * Gets a Protocol Buffers representation of the HeapConfig.
     *
     * @return A Protocol Buffers serialized representation of the HeapConfig.
     */
    public CassandraProtos.HeapConfig toProto() {
        return CassandraProtos.HeapConfig.newBuilder()
                .setSizeMb(sizeMb)
                .setNewMb(newMb)
                .build();
    }

    /**
     * Gets a byte array containing the Protocol Buffers serialized
     * representation of the HeapConfig.
     *
     * @return A byte array containing the Protocol Buffers serialized
     * representation of the HeapConfig.
     */
    public byte[] toByteArray() {

        return toProto().toByteArray();
    }

    /**
     * Gets a Map containing the environment variables used to set the heap
     * configuration of a Cassandra node.
     *
     * @return A Map containing the environment variables used to set the heap
     * configuration of the Cassandra node to the properties of the HeapConfig.
     */
    public Map<String, String> toEnv() {

        return new HashMap<String, String>() {{
            put("MAX_HEAP_SIZE", Integer.toString(sizeMb) + "M");
            put("HEAP_NEWSIZE", Integer.toString(newMb) + "M");
        }};
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HeapConfig that = (HeapConfig) o;

        if (sizeMb != that.sizeMb) return false;
        return newMb == that.newMb;

    }

    @Override
    public int hashCode() {
        int result = sizeMb;
        result = 31 * result + newMb;
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
