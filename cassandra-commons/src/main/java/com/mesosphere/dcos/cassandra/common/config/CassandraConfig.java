
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
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.Volume;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.offer.VolumeRequirement;

import java.io.IOException;
import java.util.Objects;

/**
 * CassandraConfig is the configuration object for a Cassandra node. It is
 * serializable to both JSON and Protocol Buffers.
 */
public class CassandraConfig {

    /**
     * The default configuration object.
     */
    public static final CassandraConfig DEFAULT =
            CassandraConfig.create(
                    "2.2.5",
                    0.2,
                    4096,
                    10240,
                    VolumeRequirement.VolumeType.ROOT,
                    "",
                    HeapConfig.DEFAULT,
                    Location.DEFAULT,
                    7199,
                    Volume.create("volume", 9216, ""),
                    CassandraApplicationConfig.builder().build());


    /**
     * Builder class allows for fluent construction of a new CassandraConfig
     * or construction of a new instance from the properties of an existing
     * instance.
     */
    public static class Builder {
        private String version;
        private double cpus;
        private int memoryMb;
        private int diskMb;
        private VolumeRequirement.VolumeType diskType;
        private String replaceIp;
        private HeapConfig heap;
        private Location location;
        private int jmxPort;
        private Volume volume;
        private CassandraApplicationConfig application;

        /**
         * Constructs a new Builder by copying the properties of config.
         * @param config The CassandraConfig that will be copied.
         */
        private Builder(CassandraConfig config) {

            this.version = config.version;
            this.cpus = config.cpus;
            this.memoryMb = config.memoryMb;
            this.diskMb = config.diskMb;
            this.diskType = config.diskType;
            this.replaceIp = config.replaceIp;
            this.heap = config.heap;
            this.location = config.location;
            this.jmxPort = config.jmxPort;
            this.volume = config.volume;
            this.application = config.application;
        }

        /**
         * Constructs a new Builder from the default config.
         */
        private Builder() {
            this(DEFAULT);
        }

        /**
         * Gets the application configuration.
         * @return The application configuration.
         */
        public CassandraApplicationConfig getApplication() {
            return application;
        }

        /**
         * Sets the application configuration.
         * @param application The application configuration.
         * @return The Builder instance.
         */
        public Builder setApplication(CassandraApplicationConfig application) {
            this.application = application;
            return this;
        }

        /**
         * Gets the cpu shares for the node.
         * @return The cpu shares for the node.
         */
        public double getCpus() {
            return cpus;
        }

        /**
         * Set the cpu shares for the node
         * @param cpus The cpu shares for the node.
         * @return The cpu shares for the node.
         */
        public Builder setCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        /**
         * Get the disk size for the node in Mb.
         * @return The disk size for the node in Mb.
         */
        public int getDiskMb() {
            return diskMb;
        }

        /**
         * Set the disk size for the node in Mb.
         * @param diskMb The disk size for the node in Mb.
         * @return The Builder instance.
         */
        public Builder setDiskMb(int diskMb) {
            this.diskMb = diskMb;
            return this;
        }

        /**
         * Get the disk type for the node.
         * @return The disk type for the node.
         */
        public VolumeRequirement.VolumeType getDiskType() {
            return diskType;
        }

        /**
         * Set the disk type for the node.
         * @param diskType The disk type for the node.
         * @return The Builder instance.
         */
        public Builder setDiskType(VolumeRequirement.VolumeType diskType) {
            this.diskType = diskType;
            return this;
        }

        /**
         * Get the heap configuration for the node.
         * @return The heap configuration for the node.
         */
        public HeapConfig getHeap() {
            return heap;
        }

        public Builder setHeap(HeapConfig heap) {
            this.heap = heap;
            return this;
        }

        public int getJmxPort() {
            return jmxPort;
        }

        public Builder setJmxPort(int jmxPort) {
            this.jmxPort = jmxPort;
            return this;
        }

        public Location getLocation() {
            return location;
        }

        public Builder setLocation(Location location) {
            this.location = location;
            return this;
        }

        public int getMemoryMb() {
            return memoryMb;
        }

        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        public String getReplaceIp() {
            return replaceIp;
        }

        public Builder setReplaceIp(String replaceIp) {
            this.replaceIp = replaceIp;
            return this;
        }

        public String getVersion() {
            return version;
        }

        public Builder setVersion(String version) {
            this.version = version;
            return this;
        }

        public Volume getVolume() {
            return volume;
        }

        public Builder setVolume(Volume volume) {
            this.volume = volume;
            return this;
        }

        public CassandraConfig build() {

            return create(
                    version,
                    cpus,
                    memoryMb,
                    diskMb,
                    diskType,
                    replaceIp,
                    heap,
                    location,
                    jmxPort,
                    volume,
                    application);
        }
    }

    public static final Serializer<CassandraConfig> JSON_SERIALIZER =
            new Serializer<CassandraConfig>() {
                @Override
                public byte[] serialize(CassandraConfig value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException(
                                "Error writing CassandraConfig to JSON",
                                ex);
                    }
                }

                @Override
                public CassandraConfig deserialize(byte[] bytes)
                        throws SerializationException {

                    try {
                        return JsonUtils.MAPPER.readValue(bytes,
                                CassandraConfig.class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "CassandraConfig form JSON", ex);
                    }

                }
            };


    public static Builder builder() {
        return new Builder();
    }

    @JsonCreator
    public static CassandraConfig create(
            @JsonProperty("version") String version,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("disk_mb") int diskMb,
            @JsonProperty("disk_type") VolumeRequirement.VolumeType diskType,
            @JsonProperty("replace_ip") String replaceIp,
            @JsonProperty("heap") HeapConfig heap,
            @JsonProperty("location") Location location,
            @JsonProperty("jmx_port") int jmxPort,
            @JsonProperty("volume") Volume volume,
            @JsonProperty("application")
            CassandraApplicationConfig application) {

        return new CassandraConfig(
                version,
                cpus,
                memoryMb,
                diskMb,
                diskType,
                replaceIp,
                heap,
                location,
                jmxPort,
                volume,
                application);
    }

    public static CassandraConfig parse(CassandraProtos.CassandraConfig config)
            throws IOException {

        return create(
                config.getVersion(),
                config.getCpus(),
                config.getMemoryMb(),
                config.getDiskMb(),
                VolumeRequirement.VolumeType.valueOf(config.getDiskType()),
                (config.hasReplaceIp()) ? config.getReplaceIp() : "",
                HeapConfig.parse(config.getHeap()),
                Location.parse(config.getLocation()),
                config.getJmxPort(),
                Volume.parse(config.getVolume()),
                CassandraApplicationConfig.parse(config.getApplication()));

    }

    public static CassandraConfig parse(byte[] bytes)
            throws IOException {
        return parse(CassandraProtos.CassandraConfig.parseFrom(bytes));
    }

    @JsonProperty("version")
    private final String version;

    @JsonProperty("cpus")
    private final double cpus;

    @JsonProperty("memory_mb")
    private final int memoryMb;

    @JsonProperty("disk_mb")
    private final int diskMb;

    @JsonProperty("disk_type")
    private VolumeRequirement.VolumeType diskType;

    @JsonProperty("replace_ip")
    private final String replaceIp;

    @JsonProperty("heap")
    private final HeapConfig heap;

    @JsonProperty("location")
    private final Location location;

    @JsonProperty("jmx_port")
    private final int jmxPort;

    @JsonProperty("volume")
    private final Volume volume;

    @JsonProperty("application")
    private final CassandraApplicationConfig application;

    public CassandraConfig(final String version,
                           final double cpus,
                           final int memoryMb,
                           final int diskMb,
                           final VolumeRequirement.VolumeType diskType,
                           final String replaceIp,
                           final HeapConfig heap,
                           final Location location,
                           final int jmxPort,
                           final Volume volume,
                           final CassandraApplicationConfig application) {
        this.version = version;
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.diskMb = diskMb;
        this.diskType = diskType;
        this.replaceIp = (replaceIp != null) ? replaceIp : "";
        this.heap = heap;
        this.location = location;
        this.jmxPort = jmxPort;
        this.volume = volume;
        this.application = application;
    }

    public String getVersion() {
        return version;
    }

    public String getReplaceIp() {
        return replaceIp;
    }

    public HeapConfig getHeap() {
        return heap;
    }

    public Location getLocation() {
        return location;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public CassandraApplicationConfig getApplication() {
        return application;
    }

    public double getCpus() {
        return cpus;
    }

    public int getDiskMb() {
        return diskMb;
    }

    public VolumeRequirement.VolumeType getDiskType() {
        return diskType;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public Volume getVolume() {
        return volume;
    }

    public CassandraProtos.CassandraConfig toProto()
            throws IOException {
        CassandraProtos.CassandraConfig.Builder builder =
                CassandraProtos.CassandraConfig
                        .newBuilder()
                        .setJmxPort(jmxPort)
                        .setVersion(version)
                        .setCpus(cpus)
                        .setDiskMb(diskMb)
                        .setDiskType(diskType.name())
                        .setMemoryMb(memoryMb)
                        .setReplaceIp(replaceIp)
                        .setHeap(heap.toProto())
                        .setLocation(location.toProto())
                        .setVolume(volume.toProto())
                        .setApplication(application.toByteString());

        return builder.build();
    }

    public byte[] toByteArray() throws IOException {
        return toProto().toByteArray();
    }

    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraConfig)) return false;
        CassandraConfig that = (CassandraConfig) o;
        return Double.compare(that.getCpus(), getCpus()) == 0 &&
                getMemoryMb() == that.getMemoryMb() &&
                getDiskMb() == that.getDiskMb() &&
                getDiskType() == that.getDiskType() &&
                getJmxPort() == that.getJmxPort() &&
                Objects.equals(getVersion(), that.getVersion()) &&
                Objects.equals(getReplaceIp(), that.getReplaceIp()) &&
                Objects.equals(getHeap(), that.getHeap()) &&
                Objects.equals(getLocation(), that.getLocation()) &&
                Objects.equals(getApplication(), that.getApplication());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getVersion(), getCpus(), getMemoryMb(), getDiskMb(),
                getDiskType(),
                getReplaceIp(), getHeap(), getLocation(), getJmxPort(),
                getApplication());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
