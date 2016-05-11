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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;

/**
 * Configuration object for Cassandra ClusterTasks (e.g. Cleanup, Repair,
 * Backup, Restore). It aggregates the memory, cpu, and disk allocated for
 * these tasks.
 */
public class ClusterTaskConfig {
    /**
     * The default config is 1 CPU, 256 Mb mem, and 0 disk.
     */
    public static final ClusterTaskConfig DEFAULT =
            ClusterTaskConfig.create(
                    1,
                    256,
                    0);

    /**
     * Builder class allows for fluent construction of a new ClusterTaskConfig
     * or construction of a new instance from the properties of an existing
     * instance.
     */
    public static class Builder {
        private double cpus;
        private int memoryMb;
        private int diskMb;

        /**
         * Creates a new Builder set with its properties set ot the properties
         * of config.
         * @param config The config the Builder's properties will be set to.
         */
        private Builder(ClusterTaskConfig config) {
            this.cpus = config.cpus;
            this.memoryMb = config.memoryMb;
            this.diskMb = config.diskMb;
        }

        private Builder() {
            this(DEFAULT);
        }

        /**
         * Gets the cpu shares.
         * @return The cpus shares for the cluster task.
         */
        public double getCpus() {
            return cpus;
        }

        /**
         * Sets the cpu shares.
         * @param cpus The cpu shares for the cluster task.
         * @return The Builder instance.
         */
        public Builder setCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        /**
         * Gets the allocated disk.
         * @return The disk allocated to the cluster task in Mb.
         */
        public int getDiskMb() {
            return diskMb;
        }

        /**
         * Sets the allocated disk.
         * @param diskMb The disk allocated to the cluster task in Mb.
         * @return The Builder instance.
         */
        public Builder setDiskMb(int diskMb) {
            this.diskMb = diskMb;
            return this;
        }

        /**
         * Gets the allocated memory in Mb.
         * @return The memory allocated to the cluster task in Mb.
         */
        public int getMemoryMb() {
            return memoryMb;
        }

        /**
         * Sets the allocated memory in Mb
         * @param memoryMb The memory allocated to the cluster task in Mb.
         * @return The Builder instance.
         */
        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        /**
         * Gets a ClusterTaskConfig constructed from the properties of the
         * Builder.
         * @return A ClusterTaskConfig constructed from the properties of the
         * builder.
         */
        public ClusterTaskConfig build() {

            return create(
                    cpus,
                    memoryMb,
                    diskMb);
        }
    }

    /**
     * A Serializer that serializes a ClusterTaskConfig to and from JSON.
     */
    public static final Serializer<ClusterTaskConfig> JSON_SERIALIZER =
            new Serializer<ClusterTaskConfig>() {
                @Override
                public byte[] serialize(ClusterTaskConfig value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException(
                                "Error writing ClusterTaskConfig to JSON",
                                ex);
                    }
                }

                @Override
                public ClusterTaskConfig deserialize(byte[] bytes)
                        throws SerializationException {

                    try {
                        return JsonUtils.MAPPER.readValue(bytes,
                                ClusterTaskConfig.class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "ClusterTaskConfig form JSON", ex);
                    }

                }
            };


    /**
     * Gets a Builder instance.
     * @return A Builder instance with its properties set to the default
     * configuration.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Factory method gets a new ClusterTaskConfig.
     * @param cpus The cpu shares allocated to cluster task.
     * @param memoryMb The memory allocated to the cluster task in Mb.
     * @param diskMb The disk allocated to the cluster task in Mb.
     * @return A ClusterTaskConfig with its properties set to the corresponding
     * parameters.
     */
    @JsonCreator
    public static ClusterTaskConfig create(
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("disk_mb") int diskMb) {

        return new ClusterTaskConfig(
                cpus,
                memoryMb,
                diskMb);
    }

    @JsonProperty("cpus")
    private final double cpus;

    @JsonProperty("memory_mb")
    private final int memoryMb;

    @JsonProperty("disk_mb")
    private final int diskMb;

    /**
     * Constructs a ClusterTaskConfig.
     * @param cpus The cpu shares allocated to cluster task.
     * @param memoryMb The memory allocated to the cluster task in Mb.
     * @param diskMb The disk allocated to the cluster task in Mb.
     */
    public ClusterTaskConfig(
            final double cpus,
            final int memoryMb,
            final int diskMb
    ) {
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.diskMb = diskMb;
    }

    /**
     * Gets the cpus shares allocated to the cluster task.
     * @return The cpu shares allocated to the cluster task.
     */
    public double getCpus() {
        return cpus;
    }

    /**
     * Gets the disk allocated to the cluster task.
     * @return The disk allocated to the cluster task in Mb.
     */
    public int getDiskMb() {
        return diskMb;
    }


    /**
     * Gets the memory allocated to the cluster task.
     * @return The disk allocated to the cluster task in Mb.
     */
    public int getMemoryMb() {
        return memoryMb;
    }

    /**
     * Gets a mutable Builder instance.
     * @return A mutable Builder instance whose properties are set to the
     * properties of the config.
     */
    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterTaskConfig that = (ClusterTaskConfig) o;

        if (Double.compare(that.cpus, cpus) != 0) return false;
        if (memoryMb != that.memoryMb) return false;
        return diskMb == that.diskMb;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(cpus);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + memoryMb;
        result = 31 * result + diskMb;
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}

