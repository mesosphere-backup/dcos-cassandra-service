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
import org.apache.mesos.offer.VolumeRequirement;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/**
 * CassandraConfig is the configuration object for a Cassandra node. It is
 * serializable to both JSON and Protocol Buffers.
 */
public class CassandraConfig {
    public static final String VOLUME_PATH = "volume";
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
                    false,
                    UUID.randomUUID().toString(),
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
        private boolean publishDiscoveryInfo;
        private String rollingRestartName;
        private CassandraApplicationConfig application;

        /**
         * Constructs a new Builder by copying the properties of config.
         *
         * @param config The CassandraConfig that will be copied.
         */
        public Builder(CassandraConfig config) {

            this.version = config.version;
            this.cpus = config.cpus;
            this.memoryMb = config.memoryMb;
            this.diskMb = config.diskMb;
            this.diskType = config.diskType;
            this.replaceIp = config.replaceIp;
            this.heap = config.heap;
            this.location = config.location;
            this.jmxPort = config.jmxPort;
            this.publishDiscoveryInfo = config.publishDiscoveryInfo;
            this.rollingRestartName = config.rollingRestartName;
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
         *
         * @return The application configuration.
         */
        public CassandraApplicationConfig getApplication() {
            return application;
        }

        /**
         * Sets the application configuration.
         *
         * @param application The application configuration.
         * @return The Builder instance.
         */
        public Builder setApplication(CassandraApplicationConfig application) {
            this.application = application;
            return this;
        }

        /**
         * Gets the cpu shares for the node.
         *
         * @return The cpu shares for the node.
         */
        public double getCpus() {
            return cpus;
        }

        /**
         * Set the cpu shares for the node
         *
         * @param cpus The cpu shares for the node.
         * @return The cpu shares for the node.
         */
        public Builder setCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        /**
         * Get the disk size for the node in Mb.
         *
         * @return The disk size for the node in Mb.
         */
        public int getDiskMb() {
            return diskMb;
        }

        /**
         * Set the disk size for the node in Mb.
         *
         * @param diskMb The disk size for the node in Mb.
         * @return The Builder instance.
         */
        public Builder setDiskMb(int diskMb) {
            this.diskMb = diskMb;
            return this;
        }

        /**
         * Get the disk type for the node.
         *
         * @return The disk type for the node.
         */
        public VolumeRequirement.VolumeType getDiskType() {
            return diskType;
        }

        /**
         * Set the disk type for the node.
         *
         * @param diskType The disk type for the node.
         * @return The Builder instance.
         */
        public Builder setDiskType(VolumeRequirement.VolumeType diskType) {
            this.diskType = diskType;
            return this;
        }

        /**
         * Get the heap configuration for the node.
         *
         * @return The heap configuration for the node.
         */
        public HeapConfig getHeap() {
            return heap;
        }

        /**
         * Sets the heap configuration for the node.
         * @param heap The heap configuration for the node.
         * @return The Builder instance.
         */
        public Builder setHeap(HeapConfig heap) {
            this.heap = heap;
            return this;
        }

        /**
         * Gets the JMX port for the node.
         * @return The JMX port for the node.
         */
        public int getJmxPort() {
            return jmxPort;
        }

        /**
         * Sets the JMX port for the node
         * @param jmxPort The JMX port for the node
         * @return The Builder instance.
         */
        public Builder setJmxPort(int jmxPort) {
            this.jmxPort = jmxPort;
            return this;
        }

        /**
         * Gets the Location configuration for the node.
         * @return The Location configuration for the node.
         */
        public Location getLocation() {
            return location;
        }

        /**
         * Sets the Location configuration for the node.
         * @param location The Location configuration for the node.
         * @return The Builder instance.
         */
        public Builder setLocation(Location location) {
            this.location = location;
            return this;
        }

        /**
         * Gets the memory allocated to the node in Mb.
         * @return The memory allocated to the node in Mb.
         */
        public int getMemoryMb() {
            return memoryMb;
        }

        /**
         * Sets the memory allocated to the node in Mb.
         * @param memoryMb The memory allocated to the node in Mb.
         * @return The memory allocated to the node in Mb.
         */
        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        /**
         * Gets the IP address of the node that the deployed node will
         * replace in the ring.
         * @return The IP address of the node that the deployed node will
         * replace in the ring.
         */
        public String getReplaceIp() {
            return replaceIp;
        }

        /**
         * Sets the IP address of the node that the deployed node will replace
         * in the ring.
         * @param replaceIp The IP address of the node that the deployed node
         *                  will replace in the ring.
         * @return The Builder instance.
         */
        public Builder setReplaceIp(String replaceIp) {
            this.replaceIp = replaceIp;
            return this;
        }

        /**
         * Gets the Cassandra version of the node.
         * @return The Cassandra version of the node.
         */
        public String getVersion() {
            return version;
        }

        /**
         * Sets the Cassandra version of the node.
         * @param version The Cassandra version of the node.
         * @return The Builder instance.
         */
        public Builder setVersion(String version) {
            this.version = version;
            return this;
        }

        /**
         * Gets whether the Cassandra task should publish its discovery info.
         * @return Flag that dictates whether the Cassandra task should publish its discovery info.
         */
        public boolean getPublishDiscoveryInfo() { return publishDiscoveryInfo; }

        /**
         * Sets whether the Cassandra task should publish its discovery info.
         * @param publishDiscoveryInfo Flag to enable or disable publishing of discovery info.
         * @return The Builder instance.
         */
        public Builder setPublishDiscoveryInfo(boolean publishDiscoveryInfo) {
            this.publishDiscoveryInfo = publishDiscoveryInfo;
            return this;
        }

        /**
         * Gets the value defined to perform rolling restart, without updating the actual cassandra config.
         * @return Flag that dictates whether the Cassandra task should do rolling restart
         */
        public String getRollingRestartName() {
            return rollingRestartName;
        }

        /**
         * Gets the value defined to perform rolling restart, without updating the actual cassandra config.
         * @param rollingRestartName is to enable or disable publishing of discovery info.
         * @return The Builder instance.
         */
        public Builder setRollingRestartName(String rollingRestartName) {
            this.rollingRestartName = rollingRestartName;
            return this;
        }

        /**
         * Creates a CassandraConfig with the properties of the Builder.
         * @return A
         */
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
                    publishDiscoveryInfo,
                    rollingRestartName,
                    application);
        }
    }

    /**
     * Gets a new Builder for a CassandraConfig.
     * @return A new Builder instance whose properties are set to the Default
     * instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Factory method for constructing an CassandraConfig
     * @param version The Cassanra version of the node.
     * @param cpus The cpu shares allocated to the node.
     * @param memoryMb The memory allocated to the node in Mb.
     * @param diskMb The disk allocated to the node in Mb.
     * @param diskType The type of disk for the node.
     * @param replaceIp The IP address of the node that this node will
     *                  replace in the ring (null or empty if this node will
     *                  not replace another node).
     * @param heap The heap configuration for the node.
     * @param location The location (Rack and Data center) configuration for
     *                 the node.
     * @param jmxPort The JMX port the node will listen on.
     * @param publishDiscoveryInfo The flag that specifies whether the Cassandra task should publish its discovery info.
     * @param application The Cassandra application configuration for the
     *                    node (This corresponds to the cassandra.yaml).
     * @return A CassandraConfig constructed from arguments.
     */
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
            @JsonProperty("publish_discovery_info") boolean publishDiscoveryInfo,
            @JsonProperty("rolling_restart_name") String rollingRestartName,
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
                publishDiscoveryInfo,
                rollingRestartName,
                application);
    }

    /**
     * Parses a CassandraConfig from a Protocol Buffers format.
     * @param config The Protocol Buffers serialized instance.
     * @return A CassandraConfig parsed from config.
     * @throws IOException If a valid CassandraConfig could not be parsed
     * from config.
     */
    public static CassandraConfig parse(CassandraProtos.CassandraConfig config)
            throws IOException {

        return create(
                config.getVersion(),
                config.getCpus(),
                config.getMemoryMb(),
                config.getDiskMb(),
                VolumeRequirement.VolumeType.values()[config.getDiskType()],
                (config.hasReplaceIp()) ? config.getReplaceIp() : "",
                HeapConfig.parse(config.getHeap()),
                Location.parse(config.getLocation()),
                config.getJmxPort(),
                config.getPublishDiscoveryInfo(),
                config.getRollingRestartName(),
                CassandraApplicationConfig.parse(config.getApplication()));

    }

    /**
     * Parses a CassandraConfig from a byte array containing a Protocol Buffers
     * serialized representation of the configuration.
     * @param bytes A byte array containing a Protocol Buffers serialized
     *              representation of a CassandraConfig
     * @return A CassandraConfig parsed form bytes.
     * @throws IOException If a CassandraConfig could not be parsed from bytes.
     */
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

    @JsonProperty("publish_discovery_info")
    private final boolean publishDiscoveryInfo;

    @JsonProperty("application")
    private final CassandraApplicationConfig application;

    @JsonProperty("rolling_restart_name")
    public String rollingRestartName;

    /**
     * Constructs a CassandraConfig
     * @param version The Cassanra version of the node.
     * @param cpus The cpu shares allocated to the node.
     * @param memoryMb The memory allocated to the node in Mb.
     * @param diskMb The disk allocated to the node in Mb.
     * @param diskType The type of disk for the node.
     * @param replaceIp The IP address of the node that this node will
     *                  replace in the ring (null or empty if this node will
     *                  not replace another node).
     * @param heap The heap configuration for the node.
     * @param location The location (Rack and Data center) configuration for
     *                 the node.
     * @param jmxPort The JMX port the node will listen on.
     * @param application The Cassandra application configuration for the
     *                    node (This corresponds to the cassandra.yaml).
     */
    public CassandraConfig(final String version,
                           final double cpus,
                           final int memoryMb,
                           final int diskMb,
                           final VolumeRequirement.VolumeType diskType,
                           final String replaceIp,
                           final HeapConfig heap,
                           final Location location,
                           final int jmxPort,
                           final boolean publishDiscoveryInfo,
                           final String rollingRestartName,
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
        this.publishDiscoveryInfo = publishDiscoveryInfo;
        this.rollingRestartName = (rollingRestartName != null) ? rollingRestartName : "";
        this.application = application;
    }

    /**
     * Gets the Cassandra version.
     * @return The Cassandra version for the node.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Gets the IP address of the node that will be replaced in the ring.
     * @return The IP address of the node that will be replaced in the ring or
     * the empty String if the node does not replace another node.
     */
    public String getReplaceIp() {
        return replaceIp;
    }

    /**
     * Gets the Heap configuration for the node.
     * @return The Heap configuration for the node.
     */
    public HeapConfig getHeap() {
        return heap;
    }

    /**
     * Gets the Location configuration for the node.
     * @return The Location configuration for the node. This indicates the rack
     * and data center for the node.
     */
    public Location getLocation() {
        return location;
    }

    /**
     * Gets the JMX port for the node.
     * @return The JMX port the node will listen on.
     */
    public int getJmxPort() {
        return jmxPort;
    }

    /**
     * Gets the Cassandra application configuration for the node.
     * @return The Cassandra application configuration for the node. This sets
     * the properties configured in cassandra.yaml.
     */
    public CassandraApplicationConfig getApplication() {
        return application;
    }

    /**
     * Gets the cpu shares allocated to the node.
     * @return The cpu shares allocated to the node.
     */
    public double getCpus() {
        return cpus;
    }

    /**
     * Gets the disk allocated to the node in Mb.
     * @return The amount of disk allocated to the node in Mb.
     */
    public int getDiskMb() {
        return diskMb;
    }

    /**
     * Gets the type of disk that will be used by the node.
     * @return The type of disk that will be used by the node (either Mount or
     * Root).
     */
    public VolumeRequirement.VolumeType getDiskType() {
        return diskType;
    }

    /**
     * Gets the memory allocated to the node in Mb.
     * @return The memory allocated to the node in Mb.
     */
    public int getMemoryMb() {
        return memoryMb;
    }

    /**
     * Gets whether the Cassandra task should publish its discovery info.
     * @return Flag that dictates whether the Cassandra task should publish its discovery info.
     */
    public boolean getPublishDiscoveryInfo() { return publishDiscoveryInfo; }

    /**
     * Gets whether the Cassandra task should do rolling restart.
     * @return Flag that dictates whether the Cassandra task should do rolling restart.
     */
    public String getRollingRestartName() {
        return rollingRestartName;
    }

    /**
     * Gets a Protocol Buffers representation of the config.
     * @return A Protocol Buffers representation of the config.
     * @throws IOException If the config could not be serialized. (This should
     * not throw).
     */
    public CassandraProtos.CassandraConfig toProto() {
        CassandraProtos.CassandraConfig.Builder builder =
                CassandraProtos.CassandraConfig
                        .newBuilder()
                        .setJmxPort(jmxPort)
                        .setVersion(version)
                        .setCpus(cpus)
                        .setDiskMb(diskMb)
                        .setDiskType(diskType.ordinal())
                        .setMemoryMb(memoryMb)
                        .setReplaceIp(replaceIp)
                        .setHeap(heap.toProto())
                        .setLocation(location.toProto())
                        .setPublishDiscoveryInfo(publishDiscoveryInfo)
                        .setRollingRestartName(rollingRestartName)
                        .setApplication(application.toByteString());

        return builder.build();
    }

    /**
     * Gets a byte array containing a Protocol Buffers serialized
     * representation of the CassandraConfig.
     * @return A byte array containing a Protocol Buffers serialized
     * representation of the CassandraConfig.
     * @throws IOException
     */
    public byte[] toByteArray(){
        return toProto().toByteArray();
    }

    /**
     * Gets a Builder representing a Mutable instance of the CassandraConfig.
     * @return A Builder whose properties are set to the properties of the
     * CassandraConfig.
     */
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
                getPublishDiscoveryInfo() == that.getPublishDiscoveryInfo() &&
                Objects.equals(getRollingRestartName(), that.getRollingRestartName()) &&
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
                getReplaceIp(), getHeap(), getLocation(), getJmxPort(), getPublishDiscoveryInfo(),
                getRollingRestartName(), getApplication());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
