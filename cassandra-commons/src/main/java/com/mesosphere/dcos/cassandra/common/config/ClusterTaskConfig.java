package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;

public class ClusterTaskConfig {
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

        private Builder(ClusterTaskConfig config) {
            this.cpus = config.cpus;
            this.memoryMb = config.memoryMb;
            this.diskMb = config.diskMb;
        }

        private Builder() {
            this(DEFAULT);
        }

        public double getCpus() {
            return cpus;
        }

        public Builder setCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        public int getDiskMb() {
            return diskMb;
        }

        public Builder setDiskMb(int diskMb) {
            this.diskMb = diskMb;
            return this;
        }

        public int getMemoryMb() {
            return memoryMb;
        }

        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        public ClusterTaskConfig build() {

            return create(
                    cpus,
                    memoryMb,
                    diskMb);
        }
    }

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


    public Builder builder() {
        return new Builder();
    }

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

    public ClusterTaskConfig(
            final double cpus,
            final int memoryMb,
            final int diskMb
    ) {
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.diskMb = diskMb;
    }

    public double getCpus() {
        return cpus;
    }

    public int getDiskMb() {
        return diskMb;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

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

