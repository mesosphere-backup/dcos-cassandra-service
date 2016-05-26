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
package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.offer.VolumeRequirement;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.mesos.protobuf.ResourceBuilder.*;

/**
 * CassandraDaemonTask extends CassandraTask to implement the task for a
 * Cassandra daemon. This is the task that starts and monitors the Cassandra
 * node. It is the first task launched on slave with a new executor, and it
 * must be running for additional tasks to run successfully on the slave. In
 * addition to the basic CassandraTask properties it contains the configuration
 * for the Cassandra node.
 */
public class CassandraDaemonTask extends CassandraTask {

    /**
     * Builder is a mutable instance of a CassandraDaemonTask.
     */
    public static class Builder {

        private String id;
        private String slaveId;
        private String hostname;
        private CassandraTaskExecutor executor;
        private String name;
        private String role;
        private String principal;
        private double cpus;
        private int memoryMb;
        private int diskMb;
        private CassandraDaemonStatus status;
        private CassandraConfig config;

        private Builder(CassandraDaemonTask task) {

            this.id = task.id;
            this.slaveId = task.slaveId;
            this.hostname = task.hostname;
            this.executor = task.executor;
            this.name = task.name;
            this.role = task.role;
            this.principal = task.principal;
            this.cpus = task.cpus;
            this.memoryMb = task.memoryMb;
            this.diskMb = task.diskMb;
            this.status = task.getStatus();
            this.config = task.config;

        }

        /**
         * Gets the config.
         *
         * @return The CassandraConfig for the Cassandra daemon.
         */
        public CassandraConfig getConfig() {
            return config;
        }

        /**
         * Sets the config.
         *
         * @param config The CassandraConfig for the Cassandra daemon.
         * @return The Builder instance.
         */
        public Builder setConfig(CassandraConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Sets the cpu shares for the task.
         *
         * @return The cpu shares for the task.
         */
        public double getCpus() {
            return cpus;
        }

        /**
         * Sets the cpu shares for the task.
         *
         * @param cpus The cpu shares for the task.
         * @return The Builder instance.
         */
        public Builder setCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        /**
         * Gets the disk allocation.
         *
         * @return The disk allocated for the task in Mb.
         */
        public int getDiskMb() {
            return diskMb;
        }

        /**
         * Gets the disk allocation.
         *
         * @param diskMb The disk allocated for the task in Mb.
         * @return The Builder instance.
         */
        public Builder setDiskMb(int diskMb) {
            this.diskMb = diskMb;
            return this;
        }

        /**
         * Gets the executor.
         *
         * @return The executor for the slave on which the task will be
         * launched.
         */
        public CassandraTaskExecutor getExecutor() {
            return executor;
        }

        /**
         * Sets the executor.
         *
         * @param executor The executor for the slave on which the task will
         *                 be launched.
         * @return The Builder instance.
         */
        public Builder setExecutor(CassandraTaskExecutor executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Gets the hostname.
         *
         * @return The hostname of the slave on which the task is launched.
         */
        public String getHostname() {
            return hostname;
        }

        /**
         * Sets the hostname.
         *
         * @param hostname The hostname of the slave on which the task is
         *                 launched.
         * @return The Builder instance.
         */
        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /**
         * Gets the unique id.
         *
         * @return The unique identifier of the task.
         */
        public String getId() {
            return id;
        }

        /**
         * Sets the unique id.
         *
         * @param id The unique identifier of the task.
         * @return The Builder instance.
         */
        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        /**
         * Gets the memory allocation.
         *
         * @return The memory allocation for the task in Mb.
         */
        public int getMemoryMb() {
            return memoryMb;
        }

        /**
         * Sets the memory allocation.
         *
         * @param memoryMb The memory allocation for the task in Mb.
         * @return The Builder instance.
         */
        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        /**
         * Gets the name.
         *
         * @return The name of the task.
         */
        public String getName() {
            return name;
        }

        /**
         * Sets the name.
         *
         * @param name The name of the task.
         * @return The Builder instance.
         */
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Gets the principal
         *
         * @return The principal for the task.
         */
        public String getPrincipal() {
            return principal;
        }

        /**
         * Sets the principal.
         *
         * @param principal The principal for the task.
         * @return The Builder instance.
         */
        public Builder setPrincipal(String principal) {
            this.principal = principal;
            return this;
        }

        /**
         * Gets the role.
         *
         * @return The role for the task.
         */
        public String getRole() {
            return role;
        }

        /**
         * Sets the role.
         *
         * @param role The role for the task.
         * @return The Builder instance.
         */
        public Builder setRole(String role) {
            this.role = role;
            return this;
        }

        /**
         * Gets the slave id.
         *
         * @return The unique identifier of the slave the task was launched on.
         */
        public String getSlaveId() {
            return slaveId;
        }

        /**
         * Sets the slave id.
         *
         * @param slaveId The unique identifier of the slave the task was
         *                launched on.
         * @return The Builder instance.
         */
        public Builder setSlaveId(String slaveId) {
            this.slaveId = slaveId;
            return this;
        }

        /**
         * Gets the status for the task.
         *
         * @return The status for the task.
         */
        public CassandraTaskStatus getStatus() {
            return status;
        }

        /**
         * Sets the status for the task.
         *
         * @param status The status for the task.
         * @return The Builder instance.
         */
        public Builder setStatus(CassandraDaemonStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Creates a CassandraDaemonTask for the builder's properties.
         *
         * @return A CassandraDaemonTask whose properties are set to the
         * properties of the Builder instance.
         */
        public CassandraDaemonTask build() {
            return create(
                    id,
                    slaveId,
                    hostname,
                    executor,
                    name,
                    role,
                    principal,
                    cpus,
                    memoryMb,
                    diskMb,
                    config,
                    status
            );
        }
    }

    /**
     * String prefix for the CassandraDaemon task.
     */
    public static final String NAME_PREFIX = "node-";

    /**
     * Creates a new CassandraDaemonTask.
     *
     * @param id        The unique identifier of the task.
     * @param slaveId   The identifier of the slave the task is running on.
     * @param hostname  The hostname of the slave the task is running on.
     * @param executor  The executor configuration for the task.
     * @param name      The name of the task.
     * @param role      The role for the task.
     * @param principal The principal associated with the task.
     * @param cpus      The cpu shares allocated to the task.
     * @param memoryMb  The memory allocated to the task in Mb.
     * @param diskMb    The disk allocated to the task in Mb.
     * @param config    The CassandraConfig for the task.
     * @param status    The status associated with the task.
     * @return A new CassandraDaemonTask constructed from the parameters
     */
    @JsonCreator
    public static CassandraDaemonTask create(
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("executor") CassandraTaskExecutor executor,
            @JsonProperty("name") String name,
            @JsonProperty("role") String role,
            @JsonProperty("principal") String principal,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("disk_mb") int diskMb,
            @JsonProperty("config") CassandraConfig config,
            @JsonProperty("status") CassandraDaemonStatus status) {

        return new CassandraDaemonTask(
                id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                config.getDiskType(),
                config,
                status
        );

    }

    @JsonProperty("config")
    private final CassandraConfig config;

    private final CassandraConfig updateConfig(CassandraDaemonStatus status) {
        if (Protos.TaskState.TASK_RUNNING.equals(status.getState())) {
            return config.mutable().setReplaceIp("").build();
        } else {
            return config;
        }
    }

    /**
     * Constructs a new CassandraDaemonTask.
     *
     * @param id        The unique identifier of the task.
     * @param slaveId   The identifier of the slave the task is running on.
     * @param hostname  The hostname of the slave the task is running on.
     * @param executor  The executor configuration for the task.
     * @param name      The name of the task.
     * @param role      The role for the task.
     * @param principal The principal associated with the task.
     * @param cpus      The cpu shares allocated to the task.
     * @param memoryMb  The memory allocated to the task in Mb.
     * @param diskMb    The disk allocated to the task in Mb.
     * @param config    The CassandraConfig for the task.
     * @param status    The status associated with the task.
     */
    protected CassandraDaemonTask(String id,
                                  String slaveId,
                                  String hostname,
                                  CassandraTaskExecutor executor,
                                  String name,
                                  String role,
                                  String principal,
                                  double cpus,
                                  int memoryMb,
                                  int diskMb,
                                  VolumeRequirement.VolumeType diskType,
                                  CassandraConfig config,
                                  CassandraDaemonStatus status) {

        super(TYPE.CASSANDRA_DAEMON,
                id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                diskType,
                status);

        this.config = config;
    }

    /**
     * Gets the CassandraConfig for the Cassandra daemon.
     * @return The configuration object for the Cassandra daemon.
     */
    public CassandraConfig getConfig() {
        return config;
    }

    @Override
    public CassandraProtos.CassandraTaskData getTaskData() {
        try {
            return CassandraProtos.CassandraTaskData.newBuilder()
                    .setType(
                            CassandraProtos.CassandraTaskData.TYPE.CASSANDRA_DAEMON)
                    .setConfig(config.toProto())
                    .setAddress(hostname).build();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public CassandraDaemonTask update(CassandraTaskStatus status) {

        if (status.getType() == TYPE.CASSANDRA_DAEMON &&
                status.getId().equals(id)) {

            return create(
                    id,
                    slaveId,
                    hostname,
                    executor,
                    name,
                    role,
                    principal,
                    cpus,
                    memoryMb,
                    diskMb,
                    updateConfig((CassandraDaemonStatus) status),
                    (CassandraDaemonStatus) status);
        }
        return this;

    }

    @Override
    public CassandraDaemonTask update(Protos.TaskState state) {


        return create(
                id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                config,
                ((CassandraDaemonStatus) status).update(state));

    }

    @Override
    public CassandraDaemonStatus getStatus() {
        return (CassandraDaemonStatus) status;
    }

    /**
     * Gets a mutable representation of the task.
     * @return A mutable Builder whose properties are set to the properties
     * of the task.
     */
    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public CassandraDaemonTask update(Protos.Offer offer) {
        return create(
                id,
                offer.getSlaveId().getValue(),
                offer.getHostname(),
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                config,
                CassandraDaemonStatus.create(
                        Protos.TaskState.TASK_STAGING,
                        id,
                        offer.getSlaveId().getValue(),
                        name,
                        Optional.empty(),
                        CassandraMode.STARTING));
    }

    @Override
    public CassandraDaemonTask updateId(String id) {
        return create(
                id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                config,
                (CassandraDaemonStatus) status);
    }

    @Override
    public List<Resource> getReserveResources() {
        return Arrays.asList(

                reservedPorts(PortRange.fromPorts(
                        executor.getApiPort(),
                        config.getJmxPort(),
                        config.getApplication().getStoragePort(),
                        config.getApplication().getSslStoragePort(),
                        config.getApplication().getRpcPort(),
                        config.getApplication().getNativeTransportPort())
                        .stream().map(range -> range.toProto()).collect(
                                Collectors.toList()), role, principal));
    }


    @Override
    public List<Resource> getCreateResources() {
        return Arrays.asList(config.getVolume().toResource(role, principal));
    }

    @Override
    public List<Resource> getLaunchResources() {
        return Arrays.asList(reservedCpus(cpus, role, principal),
                reservedMem(memoryMb, role, principal));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraDaemonTask)) return false;
        CassandraDaemonTask that = (CassandraDaemonTask) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    @Override
    public int getNativeTransportPort() { return config.getApplication().getNativeTransportPort(); }

    @Override
    public Protos.TaskInfo toProto() {
        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder();
        Protos.Ports.Builder discoveryPorts = Protos.Ports.newBuilder();
        discoveryPorts.addPorts(0, Protos.Port.newBuilder().setNumber(getNativeTransportPort()).setName("NativeTransport"));
        discovery.setPorts(discoveryPorts);
        discovery.setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);
        discovery.setName(config.getApplication().getClusterName() + "." + name);

        return Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId))
                .setName(name)
                .setData(getTaskData().toByteString())
                .addAllResources(getReserveResources())
                .addAllResources(getCreateResources())
                .addAllResources(getLaunchResources())
                .setExecutor(executor.toExecutorInfo(role, principal))
                .setDiscovery(discovery.build())
                .build();
    }
}
