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
 * Restore SnapshotTask extends CassandraTask to implement a task that
 * restores the snapshots of a set of key spaces and column families for a
 * Cassandra cluster to a node. The task can only be launched successfully if
 * the CassandraDaemonTask is running on the targeted slave and
 * DownloadSnaptshotTask has successfully completed.
 */
public class RestoreSnapshotTask extends CassandraTask {

    /**
     * Prefix for the name of RestoreSnapshotTasks.
     */
    public static final String NAME_PREFIX = "restore-";

    /**
     * Gets the name of a RestoreSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  RestoreSnapshotTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a RestoreSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the  RestoreSnapshotTask for daemon.
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
        private RestoreSnapshotStatus status;
        private String backupName;
        private String externalLocation;
        private String accountId;
        private String secretKey;
        private String localLocation;

        private Builder(RestoreSnapshotTask task) {

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
            this.backupName = task.backupName;
            this.externalLocation = task.externalLocation;
            this.accountId = task.accountId;
            this.secretKey = task.secretKey;
            this.localLocation = task.localLocation;
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
         * Gets the local location.
         *
         * @return The location on the host where the download will be stored.
         */
        public String getLocalLocation() {
            return localLocation;
        }

        /**
         * Sets the local location.
         *
         * @param localLocation The location on the host where the download
         *                      will be stored.
         * @return The Builder instance.
         */
        public Builder setLocalLocation(String localLocation) {
            this.localLocation = localLocation;
            return this;
        }

        /**
         * Gets the access key.
         *
         * @return The access key for the S3 bucket for backup.
         */
        public String getAccountId() {
            return accountId;
        }

        /**
         * Sets the access key.
         *
         * @param accountId The access key for the S3 bucket for the backup.
         * @return The Builder instance.
         */
        public Builder setAccountId(String accountId) {
            this.accountId = accountId;
            return this;
        }

        /**
         * Gets the secret key.
         *
         * @return The secret key for the S3 bucket for the backup.
         */
        public String getSecretKey() {
            return secretKey;
        }

        /**
         * Sets the secret key.
         *
         * @param secretKey The secret key for the S3 bucket for the backup.
         * @return The Builder instance.
         */
        public Builder setSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        /**
         * Gets the status.
         * @return The status of the snapshot restore.
         */
        public RestoreSnapshotStatus getStatus() {
            return status;
        }

        /**
         * Sets the status
         * @param status The status of the snapshot restore.
         * @return The Builder instance.
         */
        public Builder setStatus(RestoreSnapshotStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Creates a new RestoreSnapshotTask.
         * @return A RestoreSnapshotTask constructed from the properties of
         * the Builder.
         */
        public RestoreSnapshotTask build() {
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
                    backupName,
                    externalLocation,
                    accountId,
                    secretKey,
                    localLocation);
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
    }

    @JsonProperty("backup_name")
    private final String backupName;

    @JsonProperty("local_location")
    private final String localLocation;

    @JsonProperty("external_location")
    private final String externalLocation;

    @JsonProperty("account_id")
    private final String accountId;

    @JsonProperty("secret_key")
    private final String secretKey;

    /**
     * Creates a new RestoreSnapshotTask.
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
     *                         all non-system key spaces will be backed up.
     * @param externalLocation The location of the S3 bucket where the backup
     *                         will is stored.
     * @param localLocation    The location where the download will be stored on
     *                         the local host.
     * @param backupName       The name of the backup.
     * @param accountId      The S3 access key of the bucket where the backup is
     *                         stored.
     * @param secretKey      The S3 secret key of the bucket where the backup is
     *                         stored.
     * @return A new RestoreSnapshotTask constructed from the parameters.
     */
    @JsonCreator
    public static RestoreSnapshotTask create(
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
            @JsonProperty("status") RestoreSnapshotStatus status,
            @JsonProperty("backup_name") String backupName,
            @JsonProperty("external_location") String externalLocation,
            @JsonProperty("account_id") String accountId,
            @JsonProperty("secret_key") String secretKey,
            @JsonProperty("local_location") String localLocation) {
        return new RestoreSnapshotTask(id,
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
                backupName,
                externalLocation,
                accountId,
                secretKey,
                localLocation);
    }

    /**
     * Constructs a new RestoreSnapshotTask.
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
     *                         all non-system key spaces will be backed up.
     * @param externalLocation The location of the S3 bucket where the backup
     *                         will is stored.
     * @param localLocation    The location where the download will be stored on
     *                         the local host.
     * @param backupName       The name of the backup.
     * @param accountId      The S3 access key of the bucket where the backup is
     *                         stored.
     * @param secretKey      The S3 secret key of the bucket where the backup is
     *                         stored.
     */
    protected RestoreSnapshotTask(
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
            RestoreSnapshotStatus status,
            String backupName,
            String externalLocation,
            String accountId,
            String secretKey,
            String localLocation) {
        super(TYPE.SNAPSHOT_RESTORE,
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

        this.backupName = backupName;
        this.externalLocation = externalLocation;
        this.accountId = accountId;
        this.secretKey = secretKey;
        this.localLocation = localLocation;
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
     * Gets the access key.
     * @return The access key for the S3 bucket where the backup will be stored.
     */
    public String getAccountId() {
        return accountId;
    }

    /**
     * Gets the secret key.
     * @return The secret key for the S3 bucket where the backup will be stored.
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Gets the local location.
     * @return The location on the local host where the downloaded backup
     * will be stored.
     */
    public String getLocalLocation() {
        return localLocation;
    }

    @Override
    public CassandraProtos.CassandraTaskData getTaskData() {
        return CassandraProtos.CassandraTaskData.newBuilder()
                .setType(
                        CassandraProtos.CassandraTaskData.TYPE.SNAPSHOT_RESTORE)
                .setBackupName(backupName)
                .setExternalLocation(externalLocation)
                .setLocalLocation(localLocation)
                .setAccoundId(accountId)
                .setSecretKey(secretKey)
                .build();
    }

    @Override
    public RestoreSnapshotTask update(Protos.Offer offer) {
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
                (RestoreSnapshotStatus) status,
                backupName,
                externalLocation,
                accountId,
                secretKey,
                localLocation);
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
                ((RestoreSnapshotStatus) status),
                backupName,
                externalLocation,
                accountId,
                secretKey,
                localLocation);
    }

    @Override
    public RestoreSnapshotTask update(Protos.TaskState state) {
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
                ((RestoreSnapshotStatus) status).update(state),
                backupName,
                externalLocation,
                accountId,
                secretKey,
                localLocation);
    }

    @Override
    public RestoreSnapshotTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.SNAPSHOT_RESTORE &&
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
                    (RestoreSnapshotStatus) status,
                    backupName,
                    externalLocation,
                    accountId,
                    secretKey,
                    localLocation);
        } else {
            return this;
        }
    }

    /**
     * Gets a mutable Builder.
     * @return A Builder constructed with the properties of the task.
     */
    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public RestoreSnapshotStatus getStatus() {

        return (RestoreSnapshotStatus) status;
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