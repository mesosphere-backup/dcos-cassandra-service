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
package com.mesosphere.dcos.cassandra.common.tasks.backup;

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
 * BackupSnapshotTask extends CassandraTask to implement a task that
 * snapshots a set of key spaces and column families for a Cassandra cluster.
 * The task can only be launched successfully if the CassandraDaemonTask is
 * running on the targeted slave.
 * If the key spaces for the task are empty. All non-system key spaces are
 * backed up.
 * If the column families for the task are empty. All column families for the
 * indicated key spaces are backed up.
 */
public class BackupSnapshotTask extends CassandraTask {

    /**
     * The name prefix for BackupSnapshotTasks.
     */
    public static final String NAME_PREFIX = "snapshot-";

    /**
     * Gets the name of a BackupSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the BackupSnapshotTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a BackupSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               taken.
     * @return The name of the BackupSnapshotTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    /**
     * Builder for fluent style construction and mutation of
     * BackupSnapshotTasks.
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
        private BackupSnapshotStatus status;
        private List<String> keySpaces;
        private List<String> columnFamilies;
        private String backupName;
        private String externalLocation;
        private String s3AccessKey;
        private String s3SecretKey;

        private Builder(BackupSnapshotTask task) {
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
            this.backupName = task.backupName;
            this.externalLocation = task.externalLocation;
            this.s3AccessKey = task.s3AccessKey;
            this.s3SecretKey = task.s3SecretKey;
        }

        /**
         * Gets the name of the backup.
         *
         * @return The name of the Backup.
         */
        public String getBackupName() {
            return backupName;
        }

        /**
         * Sets the name of the backup.
         *
         * @param backupName The name of the backup.
         * @return The Builder instance.
         */
        public Builder setBackupName(String backupName) {
            this.backupName = backupName;
            return this;
        }

        /**
         * Gets the column families.
         *
         * @return A List of the names for the column families that will be
         * backed up.
         */
        public List<String> getColumnFamilies() {
            return columnFamilies;
        }

        /**
         * Sets the column families.
         *
         * @param columnFamilies A List of the names of the column families
         *                       that will be backed up.
         * @return The Builder instance.
         */
        public Builder setColumnFamilies(List<String> columnFamilies) {
            this.columnFamilies = columnFamilies;
            return this;
        }

        /**
         * Gets the external location.
         *
         * @return The location of the S3 bucket where the backup will be
         * stored.
         */
        public String getExternalLocation() {
            return externalLocation;
        }

        /**
         * Sets the external location.
         *
         * @param externalLocation The location of the S3 bucket where the
         *                         backup will be stored.
         * @return The Builder instance.
         */
        public Builder setExternalLocation(String externalLocation) {
            this.externalLocation = externalLocation;
            return this;
        }

        /**
         * Gets the key spaces.
         *
         * @return A List of the key spaces that will be backed up.
         */
        public List<String> getKeySpaces() {
            return keySpaces;
        }

        /**
         * Sets the key spaces.
         *
         * @param keySpaces A List of the key spaces that will be backed up.
         * @return The Builder instance.
         */
        public Builder setKeySpaces(List<String> keySpaces) {
            this.keySpaces = keySpaces;
            return this;
        }

        /**
         * Gets the access key.
         *
         * @return The access key for the S3 bucket for backup.
         */
        public String getS3AccessKey() {
            return s3AccessKey;
        }

        /**
         * Sets the access key.
         *
         * @param s3AccessKey The access key for the S3 bucket for the backup.
         * @return The Builder instance.
         */
        public Builder setS3AccessKey(String s3AccessKey) {
            this.s3AccessKey = s3AccessKey;
            return this;
        }

        /**
         * Gets the secret key.
         *
         * @return The secret key for the S3 bucket for the backup.
         */
        public String getS3SecretKey() {
            return s3SecretKey;
        }

        /**
         * Sets the secret key.
         *
         * @param s3SecretKey The secret key for the S3 bucket for the backup.
         * @return The Builder instance.
         */
        public Builder setS3SecretKey(String s3SecretKey) {
            this.s3SecretKey = s3SecretKey;
            return this;
        }

        /**
         * Gets the status of the task.
         *
         * @return The status of the task.
         */
        public BackupSnapshotStatus getStatus() {
            return status;
        }

        /**
         * Sets the status of the task.
         *
         * @param status The status of the task.
         * @return Teh Builder instance.
         */
        public Builder setStatus(BackupSnapshotStatus status) {
            this.status = status;
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
         * Creates a BackupSnapshotTask from the Builder.
         *
         * @return A BackupSnapshotTask constructed from the properties of
         * the Builder.
         */
        public BackupSnapshotTask build() {

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
                    columnFamilies,
                    backupName,
                    externalLocation,
                    s3AccessKey,
                    s3SecretKey);
        }

    }

    @JsonProperty("key_spaces")
    private final List<String> keySpaces;

    @JsonProperty("column_families")
    private final List<String> columnFamilies;

    @JsonProperty("backup_name")
    private final String backupName;

    @JsonProperty("external_location")
    private final String externalLocation;

    @JsonProperty("s3_access_key")
    private final String s3AccessKey;

    @JsonProperty("s3_secret_key")
    private final String s3SecretKey;

    /**
     * Creates a new BackupSnapshotTask.
     *
     * @param id               The unique identifier of the task.
     * @param slaveId          The identifier of the slave the task is running on.
     * @param hostname         The hostname of the slave the task is running on.
     * @param executor         The executor configuration for the task.
     * @param name             The name of the task.
     * @param role             The role for the task.
     * @param principal        The principal associated with the task.
     * @param cpus             The cpu shares allocated to the task.
     * @param memoryMb         The memory allocated to the task in Mb.
     * @param diskMb           The disk allocated to the task in Mb.
     * @param status           The status associated with the task.
     * @param columnFamilies   The column families that will be backed up. If
     *                         empty all valid column families will be backed up.
     * @param keySpaces        The keyspaces that will be backed up. If empty
     *                         all non-system key spaces will be backed up.
     * @param externalLocation The location of the S3 bucket where the backup
     *                         will be stored.
     * @param backupName       The name of the backup.
     * @param s3AccessKey      The S3 access key of the bucket where the backup is
     *                         stored.
     * @param s3SecretKey      The S3 secret key of the bucket where the backup is
     *                         stored.
     * @return A new BackupSnapshotTask constructed from the parameters.
     */
    @JsonCreator
    public static BackupSnapshotTask create(
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
            @JsonProperty("status") BackupSnapshotStatus status,
            @JsonProperty("key_spaces") List<String> keySpaces,
            @JsonProperty("column_families") List<String> columnFamilies,
            @JsonProperty("backup_name") String backupName,
            @JsonProperty("external_location") String externalLocation,
            @JsonProperty("s3_access_key") String s3AccessKey,
            @JsonProperty("s3_secret_key") String s3SecretKey) {


        return new BackupSnapshotTask(id,
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
                columnFamilies,
                backupName,
                externalLocation,
                s3AccessKey,
                s3SecretKey);
    }

    /**
     * Constructs a new BackupSnapshotTask.
     *
     * @param id               The unique identifier of the task.
     * @param slaveId          The identifier of the slave the task is running on.
     * @param hostname         The hostname of the slave the task is running on.
     * @param executor         The executor configuration for the task.
     * @param name             The name of the task.
     * @param role             The role for the task.
     * @param principal        The principal associated with the task.
     * @param cpus             The cpu shares allocated to the task.
     * @param memoryMb         The memory allocated to the task in Mb.
     * @param diskMb           The disk allocated to the task in Mb.
     * @param status           The status associated with the task.
     * @param columnFamilies   The column families that will be backed up. If
     *                         empty all valid column families will be backed up.
     * @param keySpaces        The keyspaces that will be backed up. If empty
     *                         all non-system key spaces will be backed up.
     * @param externalLocation The location of the S3 bucket where the backup
     *                         will be stored.
     * @param backupName       The name of the backup.
     * @param s3AccessKey      The S3 access key of the bucket where the backup is
     *                         stored.
     * @param s3SecretKey      The S3 secret key of the bucket where the backup is
     *                         stored.
     */
    protected BackupSnapshotTask(
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
            BackupSnapshotStatus status,
            List<String> keySpaces,
            List<String> columnFamilies,
            String backupName,
            String externalLocation,
            String s3AccessKey,
            String s3SecretKey) {
        super(TYPE.BACKUP_SNAPSHOT,
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
        this.backupName = backupName;
        this.externalLocation = externalLocation;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
    }

    /**
     * Gets the column families.
     * @return The column families that will be backed up. If empty, all
     * column families will be backed up.
     */
    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * Gets the name of the backup.
     * @return The name of the backup.
     */
    public String getBackupName() {
        return backupName;
    }

    /**
     * Gets the location of the S3 bucket where the backup will be stored.
     * @return The location of the S3 bucket where the backup will be stored.
     */
    public String getExternalLocation() {
        return externalLocation;
    }

    /**
     * Gets the key spaces.
     * @return The key spaces that will be backed up. If empty, all
     * non-system key spaces will be backed up.
     */
    public List<String> getKeySpaces() {
        return keySpaces;
    }

    /**
     * Gets the access key.
     * @return The access key for the S3 bucket where the backup will be stored.
     */
    public String getS3AccessKey() {
        return s3AccessKey;
    }

    /**
     * Gets the secret key.
     * @return The secret key for the S3 bucket where the backup will be stored.
     */
    public String getS3SecretKey() {
        return s3SecretKey;
    }

    @Override
    public CassandraProtos.CassandraTaskData getTaskData() {
        return CassandraProtos.CassandraTaskData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.BACKUP_SNAPSHOT)
                .addAllColumnFamilies(columnFamilies)
                .addAllKeySpaces(keySpaces)
                .setBackupName(backupName)
                .setExternalLocation(externalLocation)
                .setS3AccessKey(s3AccessKey)
                .setS3SecretKey(s3SecretKey)
                .build();
    }

    @Override
    public BackupSnapshotTask update(Protos.Offer offer) {
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
                (BackupSnapshotStatus) status,
                keySpaces,
                columnFamilies,
                backupName,
                externalLocation,
                s3AccessKey,
                s3SecretKey);
    }

    @Override
    public BackupSnapshotTask updateId(String id) {
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
                (BackupSnapshotStatus) status,
                keySpaces,
                columnFamilies,
                backupName,
                externalLocation,
                s3AccessKey,
                s3SecretKey);
    }

    @Override
    public BackupSnapshotTask update(Protos.TaskState state) {
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
                ((BackupSnapshotStatus) status).update(state),
                keySpaces,
                columnFamilies,
                backupName,
                externalLocation,
                s3AccessKey,
                s3SecretKey);

    }

    @Override
    public BackupSnapshotTask update(CassandraTaskStatus status) {

        if (status.getType() == TYPE.BACKUP_SNAPSHOT &&
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
                    (BackupSnapshotStatus) status,
                    keySpaces,
                    columnFamilies,
                    backupName,
                    externalLocation,
                    s3AccessKey,
                    s3SecretKey);
        } else {
            return this;
        }
    }

    /**
     * Gets a mutable Builder.
     * @return A mutable Builder whose properties are set to the properties
     * of the BackupSnapshotTask.
     */
    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public BackupSnapshotStatus getStatus() {

        return (BackupSnapshotStatus) status;

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
