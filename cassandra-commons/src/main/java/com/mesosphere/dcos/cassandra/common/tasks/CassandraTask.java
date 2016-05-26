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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupStatus;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairStatus;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.offer.VolumeRequirement;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.apache.mesos.offer.ResourceUtils.*;

/**
 * CassandraTask is the base class from which all framework tasks derive.
 * When new tasks are added this serializers of this class must be updated to
 * incorporate the new task type.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = CassandraTask.class,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CassandraDaemonTask.class,
                name = "CASSANDRA_DAEMON"),
        @JsonSubTypes.Type(value = BackupSnapshotTask.class, name =
                "BACKUP_SNAPSHOT"),
        @JsonSubTypes.Type(value = BackupUploadTask.class, name =
                "BACKUP_UPLOAD"),
        @JsonSubTypes.Type(value = DownloadSnapshotTask.class, name =
                "SNAPSHOT_DOWNLOAD"),
        @JsonSubTypes.Type(value = RestoreSnapshotTask.class, name =
                "SNAPSHOT_RESTORE"),
        @JsonSubTypes.Type(value = CleanupTask.class, name =
                "CLEANUP"),
        @JsonSubTypes.Type(value = RepairTask.class, name =
                "REPAIR"),
})
public abstract class CassandraTask {

    /**
     * Serializer that serializes CassandraTasks to and from JSON Objects.
     */
    public static Serializer<CassandraTask> JSON_SERIALIZER = new
            Serializer<CassandraTask>() {
                @Override
                public byte[] serialize(CassandraTask value)
                        throws SerializationException {

                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException("Error writing " +
                                "CassandraTask to JSON", ex);
                    }
                }

                @Override
                public CassandraTask deserialize(byte[] bytes)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.readValue(bytes, CassandraTask
                                .class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "CassandraTask from JSON", ex);
                    }
                }
            };

    /**
     * Enumeration of the types of Cassandra Tasks.
     */
    public enum TYPE {
        /**
         * Task that represents the Cassandra daemon.
         */
        CASSANDRA_DAEMON,
        /**
         * Task that backs up column families.
         */
        BACKUP_SNAPSHOT,
        /**
         * Task that uploads column families to remote storage.
         */
        BACKUP_UPLOAD,
        /**
         * Task that downloads snapshotted column families.
         */
        SNAPSHOT_DOWNLOAD,
        /**
         * Task that restores column families to a node.
         */
        SNAPSHOT_RESTORE,
        /**
         * Task that performs cleanup on a node.
         */
        CLEANUP,
        /**
         * Task that performs primary range, local, anti-entropy repair on a
         * node.
         */
        REPAIR
    }

    /**
     * Parses a CassandraTask from a Protocol Buffers representation.
     * @param info The TaskInfo Protocol Buffer that contains a Cassandra Task.
     * @return A CassandraTask parsed from info.
     * @throws IOException If a CassandraTask can not be parsed from info.
     */
    public static CassandraTask parse(Protos.TaskInfo info)
            throws IOException {

        CassandraProtos.CassandraTaskData data =
                CassandraProtos.CassandraTaskData.parseFrom(info
                        .getData());
        List<Resource> resources = info.getResourcesList();
        String role = resources.get(0).getRole();
        String principal = resources.get(0).getReservation().getPrincipal();

        switch (data.getType()) {

            case CASSANDRA_DAEMON:
                CassandraConfig config =
                        CassandraConfig.parse(data.getConfig());
                return CassandraDaemonTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        config.getCpus(),
                        config.getMemoryMb(),
                        config.getDiskMb(),
                        config,
                        CassandraDaemonStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty(),
                                CassandraMode.STARTING));

            case BACKUP_SNAPSHOT:
                return BackupSnapshotTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        BackupSnapshotStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList(),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey()
                );

            case BACKUP_UPLOAD:
                return BackupUploadTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        BackupUploadStatus.create(Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList(),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey(),
                        data.getLocalLocation()
                );

            case SNAPSHOT_DOWNLOAD:
                return DownloadSnapshotTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        DownloadSnapshotStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey(),
                        data.getLocalLocation()
                );

            case SNAPSHOT_RESTORE:
                return RestoreSnapshotTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        RestoreSnapshotStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey(),
                        data.getLocalLocation()
                );

            case CLEANUP:
                return CleanupTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        CleanupStatus.create(Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList()
                );

            case REPAIR:
                return RepairTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        RepairStatus.create(Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList()
                );
            default:
                return null;
        }
    }

    /**
     * Gets a unique identifier.
     * @return A universally unique identifier.
     */
    public static String uniqueId() {
        return UUID.randomUUID().toString();
    }

    protected final TYPE type;
    protected final String id;
    protected final String slaveId;
    protected final String hostname;
    protected final CassandraTaskExecutor executor;
    protected final String name;
    protected final String role;
    protected final String principal;
    protected final double cpus;
    protected final int memoryMb;
    protected final int diskMb;
    protected final VolumeRequirement.VolumeType diskType;
    protected final CassandraTaskStatus status;

    /**
     * Constructs the base CassandraTask.
     * @param type The type of the task.
     * @param id The unique identifier of the task.
     * @param slaveId The identifier of the slave the task is running on.
     * @param hostname The hostname of the slave the task is running on.
     * @param executor The executor configuration for the task.
     * @param name The name of the task.
     * @param role The role for the task.
     * @param principal The principal associated with the task.
     * @param cpus The cpu shares allocated to the task.
     * @param memoryMb The memory allocated to the task in Mb.
     * @param diskMb The disk allocated to the task in Mb.
     * @param diskType The type of disk allocated to the task.
     * @param status The status associated with the task.
     */
    protected CassandraTask(
            TYPE type,
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
            VolumeRequirement.VolumeType diskType,
            CassandraTaskStatus status) {
        this.type = type;
        this.id = id;
        this.slaveId = slaveId;
        this.hostname = hostname;
        this.executor = executor;
        this.name = name;
        this.role = role;
        this.principal = principal;
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.diskMb = diskMb;
        this.diskType = diskType;
        this.status = status;
    }

    /**
     * Gets the cpu shares allocated to the task.
     * @return The cpu shares allocated to the task.
     */
    @JsonProperty("cpus")
    public double getCpus() {
        return cpus;
    }

    /**
     * Gets the disk allocated to the task.
     * @return The disk allocated to the task in Mb.
     */
    @JsonProperty("disk_mb")
    public int getDiskMb() {
        return diskMb;
    }

    /**
     * Gets the disk type for the task.
     * @return The disk type for the task.
     */
    @JsonProperty("disk_type")
    public VolumeRequirement.VolumeType getDiskType() {
        return diskType;
    }

    /**
     * Gets the memory allocated to the task.
     * @return The memory allocated to the task in Mb.
     */
    @JsonProperty("memory_mb")
    public int getMemoryMb() {
        return memoryMb;
    }

    /**
     * Gets the unique identifier for the task.
     * @return The universally unique identifier for the task.
     */
    @JsonProperty("id")
    public String getId() {
        return id;
    }

    /**
     * Gets the task name.
     * @return The name of the task.
     */
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    /**
     * Gets the tasks principal.
     * @return The principal associated with the tasks resources.
     */
    @JsonProperty("principal")
    public String getPrincipal() {
        return principal;
    }

    /**
     * Gets the task's role.
     * @return The task's role.
     */
    @JsonProperty("role")
    public String getRole() {
        return role;
    }

    /**
     * Gets the task's status.
     * @return The status associated with the task.
     */
    @JsonProperty("status")
    public CassandraTaskStatus getStatus() {
        return status;
    }

    /**
     * Gets the executor for the task.
     * @return The CassandraTaskExecutor associated with the task.
     */
    @JsonProperty("executor")
    public CassandraTaskExecutor getExecutor() {
        return executor;
    }

    /**
     * Gets the hostname of the task. This may be empty if the task has not
     * yet been launched.
     * @return The hostname associated with the task.
     */
    @JsonProperty("hostname")
    public String getHostname() {
        return hostname;
    }

    /**
     * Gets the id of the slave the task is running on. This may be empty if the
     * task is not yet launched.
     * @return The identifier of the slave the task is running on.
     */
    @JsonProperty("slave_id")
    public String getSlaveId() {
        return slaveId;
    }

    /**
     * Gets the tasks type.
     * @return The TYPE of the task.
     */
    @JsonProperty("type")
    public TYPE getType() {
        return type;
    }

    /**
     * Gets a Protocol Buffers representation of the task.
     * @return A TaskInfo containing a Protocol Buffers representation of the
     * task.
     */
    public Protos.TaskInfo toProto() {
        return Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId))
                .setName(name)
                .setData(getTaskData().toByteString())
                .addAllResources(getReserveResources())
                .addAllResources(getCreateResources())
                .addAllResources(getLaunchResources())
                .setExecutor(executor.toExecutorInfo(role, principal))
                .build();
    }

    /**
     * Tests if the task is terminated.
     * @return True if the task is terminated.
     */
    @JsonIgnore
    public boolean isTerminated() {
        return status.isTerminated();
    }

    /**
     * Tests if the task is running.
     * @return True if the task is running.
     */
    @JsonIgnore
    public boolean isRunning() {
        return status.isRunning();
    }

    /**
     * Tests if the task is launching.
     * @return True if the task is launching.
     */
    @JsonIgnore
    public boolean isLaunching() {
        return status.isLaunching();
    }

    /**
     * Updates the task with the provided offer.
     * @param offer The Offer that will be used to launch the task.
     * @return A copy of the task with the hostname and slave id extracted
     * from offer. The task will be in the staging state.
     */
    @JsonIgnore
    public abstract CassandraTask update(Protos.Offer offer);

    /**
     * Updates the task id.
     * @param id The id for the task.
     * @return A copy of the task with its id set to id.
     */
    @JsonIgnore
    public abstract CassandraTask updateId(String id);

    /**
     * Gets the task data for the task.
     * @return A Protocol Buffers serializable representation of the task data
     * associated with the task.
     */
    @JsonIgnore
    public abstract CassandraProtos.CassandraTaskData getTaskData();

    /**
     * Updates the tasks status.
     * @param status The status that will be associated with the task.
     * @return A copy of the task with its status set to status.
     */
    @JsonIgnore
    public abstract CassandraTask update(CassandraTaskStatus status);

    /**
     * Updates the state of the task.
     * @param state The TaskState associated with the tasks' status.
     * @return A copy of the task with its status's state set to state.
     */
    public abstract CassandraTask update(Protos.TaskState state);

    /**
     * Gets the resource to reserve for the task.
     * @return A list of resources to reserve for the task.
     */
    @JsonIgnore
    public abstract List<Resource> getReserveResources();

    /**
     * Gets the resources to create for the task.
     * @return A list of resources to create for the task.
     */
    @JsonIgnore
    public abstract List<Resource> getCreateResources();

    /**
     * Gets the resources to launch for the task.
     * @return A list of resources to launch the task with.
     */
    @JsonIgnore
    public abstract List<Resource> getLaunchResources();

    @JsonIgnore
    public int getNativeTransportPort() { return -1; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTask)) return false;
        CassandraTask that = (CassandraTask) o;
        return Double.compare(that.getCpus(), getCpus()) == 0 &&
                getMemoryMb() == that.getMemoryMb() &&
                getDiskMb() == that.getDiskMb() &&
                getDiskType() == that.getDiskType() &&
                getType() == that.getType() &&
                Objects.equals(getId(), that.getId()) &&
                Objects.equals(getSlaveId(), that.getSlaveId()) &&
                Objects.equals(getHostname(), that.getHostname()) &&
                Objects.equals(getExecutor(), that.getExecutor()) &&
                Objects.equals(getName(), that.getName()) &&
                Objects.equals(getRole(), that.getRole()) &&
                Objects.equals(getPrincipal(), that.getPrincipal()) &&
                Objects.equals(getStatus(), that.getStatus());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getId(), getSlaveId(), getHostname(),
                getExecutor(), getName(), getRole(), getPrincipal(), getCpus(),
                getMemoryMb(), getDiskMb(), getDiskType(), getStatus());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
