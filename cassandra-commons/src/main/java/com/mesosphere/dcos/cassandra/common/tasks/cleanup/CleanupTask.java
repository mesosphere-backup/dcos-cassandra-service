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
package com.mesosphere.dcos.cassandra.common.tasks.cleanup;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.VolumeRequirement;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.mesos.protobuf.ResourceBuilder.*;

/**
 * CleanupTask extends CassandraTask to implement node cleanup. A
 * CassandraDaemonTask must be running on the slave for a CleanupTask to
 * successfully execute.
 * Cleanup removes all keys for a node that no longer fall in the token range
 * for the node. Cleanup should be run as a maintenance activity after node
 * addition, node removal, or node
 * replacement.
 * If the key spaces for the context are empty, all non-system key spaces are
 * used.
 * If the column families for the context are empty, all non-system column
 * families are used.
 */
public class CleanupTask extends CassandraTask {

    /**
     * The name prefix for a CleanupTask.
     */
    public static final String NAME_PREFIX = "cleanup-";

    /**
     * Gets the name of a CleanupTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  CleanupTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a CleanupTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the  CleanupTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    /**
     * Builder class for fluent style construction and mutation.
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
        private CleanupStatus status;
        private List<String> keySpaces;
        private List<String> columnFamilies;

        private Builder(CleanupTask task) {

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
            this.columnFamilies = task.columnFamilies;
            this.keySpaces = task.keySpaces;
        }

        /**
         * Gets the column families.
         *
         * @return The column families that will be cleaned up. If empty, all
         * column families will be cleaned up.
         */
        public List<String> getColumnFamilies() {
            return columnFamilies;
        }

        /**
         * Sets the column families.
         *
         * @param columnFamilies The column families that will be cleaned. If
         *                       empty, all column families will be cleaned.
         * @return The Builder instance.
         */
        public Builder setColumnFamilies(List<String> columnFamilies) {
            this.columnFamilies = columnFamilies;
            return this;
        }

        /**
         * Gets the key spaces.
         *
         * @return The key spaces that will be cleaned. If empty, all
         * non-system key spaces will be cleaned.
         */
        public List<String> getKeySpaces() {
            return keySpaces;
        }

        /**
         * Sets the key spaces.
         *
         * @param keySpaces The key spaces that will be cleaned. If empty, all
         *                  non-system key spaces will be cleaned.
         * @return The Builder instance.
         */
        public Builder setKeySpaces(List<String> keySpaces) {
            this.keySpaces = keySpaces;
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
         * Gets the status.
         *
         * @return The status of the cleanup.
         */
        public CleanupStatus getStatus() {
            return status;
        }

        /**
         * Sets the status.
         *
         * @param status The status of the cleanup.
         * @return The Builder instance.
         */
        public Builder setStatus(CleanupStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Creates a CleanupTask.
         *
         * @return A CleanupTask constructed from the properties of the Builder.
         */
        public CleanupTask build() {
            return create(id,
                    slaveId,
                    hostname,
                    executor,
                    name,
                    role,
                    principal,
                    cpus,
                    memoryMb,
                    diskMb,
                    status,
                    keySpaces,
                    columnFamilies);
        }
    }


    @JsonProperty("key_spaces")
    private final List<String> keySpaces;

    @JsonProperty("column_families")
    private final List<String> columnFamilies;

    /**
     * Creates a new CleanupTask.
     *
     * @param id             The unique identifier of the task.
     * @param slaveId        The identifier of the slave the task is running on.
     * @param hostname       The hostname of the slave the task is running on.
     * @param executor       The executor configuration for the task.
     * @param name           The name of the task.
     * @param role           The role for the task.
     * @param principal      The principal associated with the task.
     * @param cpus           The cpu shares allocated to the task.
     * @param memoryMb       The memory allocated to the task in Mb.
     * @param diskMb         The disk allocated to the task in Mb.
     * @param status         The status associated with the task.
     * @param columnFamilies The column families that will be cleaned. If
     *                       empty, all column families will be cleaned.
     * @param keySpaces      The key spaces that will be cleaned. If empty, all
     *                       non-system key spaces will be cleaned.
     * @return A CleanupTask constructed from the parameters.
     */
    @JsonCreator
    public static CleanupTask create(
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
            @JsonProperty("status") CleanupStatus status,
            @JsonProperty("key_spaces") List<String> keySpaces,
            @JsonProperty("column_families") List<String> columnFamilies) {
        return new CleanupTask(id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                status,
                keySpaces,
                columnFamilies);
    }

    /**
     * Constructs a new CleanupTask.
     *
     * @param id             The unique identifier of the task.
     * @param slaveId        The identifier of the slave the task is running on.
     * @param hostname       The hostname of the slave the task is running on.
     * @param executor       The executor configuration for the task.
     * @param name           The name of the task.
     * @param role           The role for the task.
     * @param principal      The principal associated with the task.
     * @param cpus           The cpu shares allocated to the task.
     * @param memoryMb       The memory allocated to the task in Mb.
     * @param diskMb         The disk allocated to the task in Mb.
     * @param status         The status associated with the task.
     * @param columnFamilies The column families that will be cleaned. If
     *                       empty, all column families will be cleaned.
     * @param keySpaces      The key spaces that will be cleaned. If empty, all
     *                       non-system key spaces will be cleaned.
     */
    protected CleanupTask(
            String id,
            String slaveId,
            String hostname,
            CassandraTaskExecutor executor,
            String name,
            String role,
            String principal,
            double cpus,
            int memoryMb,
            int diskMb,
            CleanupStatus status,
            List<String> keySpaces,
            List<String> columnFamilies) {
        super(CassandraTask.TYPE.CLEANUP,
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
                VolumeRequirement.VolumeType.ROOT,
                status);

        this.keySpaces = ImmutableList.copyOf(keySpaces);
        this.columnFamilies = ImmutableList.copyOf(columnFamilies);
    }

    /**
     * Gets the column families.
     * @return The column families that will be cleaned. If empty, all column
     * families will be cleaned.
     */
    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * Gets the key spaces.
     * @return The key spaces that will be cleaned. If empty, all non-system
     * key spaces will be cleaned.
     */
    public List<String> getKeySpaces() {
        return keySpaces;
    }


    @Override
    public CassandraProtos.CassandraTaskData getTaskData() {
        return CassandraProtos.CassandraTaskData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.CLEANUP)
                .addAllColumnFamilies(columnFamilies)
                .addAllKeySpaces(keySpaces)
                .build();
    }

    @Override
    public CleanupTask update(Protos.Offer offer) {
        return create(id,
                offer.getSlaveId().getValue(),
                offer.getHostname(),
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                (CleanupStatus) status,
                keySpaces,
                columnFamilies);
    }

    @Override
    public CassandraTask updateId(String id) {
        return create(id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                (CleanupStatus) status,
                keySpaces,
                columnFamilies);
    }

    @Override
    public CleanupTask update(Protos.TaskState state) {
        return create(id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                ((CleanupStatus) status).update(state),
                keySpaces,
                columnFamilies);
    }

    @Override
    public CleanupTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.CLEANUP &&
                status.getId().equals(id)) {

            return create(id,
                    slaveId,
                    hostname,
                    executor,
                    name,
                    role,
                    principal,
                    cpus,
                    memoryMb,
                    diskMb,
                    (CleanupStatus) status,
                    keySpaces,
                    columnFamilies);
        } else {
            return this;
        }
    }

    /**
     * Gets a mutable builder.
     * @return A mutable Builder object constructed from the properties of the
     * CleanupTask.
     */
    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public CleanupStatus getStatus() {
        return (CleanupStatus) status;
    }

    @Override
    public List<Protos.Resource> getReserveResources() {
        return Collections.emptyList();
    }

    @Override
    public List<Protos.Resource> getCreateResources() {
        return Collections.emptyList();
    }

    @Override
    public List<Protos.Resource> getLaunchResources() {
        return Arrays.asList(
                reservedCpus(cpus, role, principal),
                reservedMem(memoryMb, role, principal),
                reservedDisk(diskMb, role, principal));
    }

}
