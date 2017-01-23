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

import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableTask;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.DiscoveryInfo;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.offer.VolumeRequirement;
import org.apache.mesos.util.Algorithms;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * CassandraTask is the base class from which all framework tasks derive.
 * When new tasks are added this serializers of this class must be updated to
 * incorporate the new task type.
 */

public abstract class CassandraTask {

    protected static Protos.SlaveID EMPTY_SLAVE_ID = Protos.SlaveID
        .newBuilder().setValue("").build();

    /**
     * Serializer that serializes CassandraTasks to and from JSON Objects.
     */
    public static final Serializer<CassandraTask> PROTO_SERIALIZER = new
        Serializer<CassandraTask>() {
            @Override
            public byte[] serialize(CassandraTask value)
                throws SerializationException {
                return value.getTaskInfo().toByteArray();

            }

            @Override
            public CassandraTask deserialize(byte[] bytes)
                throws SerializationException {
                try {
                    return CassandraTask.parse(
                        Protos.TaskInfo.parseFrom(bytes));
                } catch (IOException ex) {
                    throw new SerializationException("Error reading " +
                        "CassandraTask from JSON", ex);
                }
            }
        };

    /**
     * Enumeration of the types of Cassandra Tasks.
     * Note: Always add new types to the end of this list so that existing tasks get serialized/deserialized correctly.
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
        REPAIR,
        /**
         * Place holder for pre-reserving resources for Cluster Tasks
         */
        TEMPLATE,
        /**
         * Task that performs upgrade SSTables on a node.
         */
        UPGRADESSTABLE,
        /**
         * Task that backup schema for cassandra daemon.
         */
        BACKUP_SCHEMA,
        /**
         * Task that restores the schema on a node.
         */
        SCHEMA_RESTORE,
    }

    /**
     * Parses a CassandraTask from a Protocol Buffers representation.
     *
     * @param info The TaskInfo Protocol Buffer that contains a Cassandra Task.
     * @return A CassandraTask parsed from info.
     * @throws IOException If a CassandraTask can not be parsed from info.
     */
    public static CassandraTask parse(final Protos.TaskInfo info) {
        CassandraData data = CassandraData.parse(info.getData());
        switch (data.getType()) {
            case CASSANDRA_DAEMON:
                return CassandraDaemonTask.parse(info);
            case BACKUP_SNAPSHOT:
                return BackupSnapshotTask.parse(info);
            case BACKUP_SCHEMA:
                return BackupSchemaTask.parse(info);
            case BACKUP_UPLOAD:
                return BackupUploadTask.parse(info);
            case SNAPSHOT_DOWNLOAD:
                return DownloadSnapshotTask.parse(info);
            case SNAPSHOT_RESTORE:
                return RestoreSnapshotTask.parse(info);
            case SCHEMA_RESTORE:
                return RestoreSchemaTask.parse(info);
            case CLEANUP:
                return CleanupTask.parse(info);
            case REPAIR:
                return RepairTask.parse(info);
            case UPGRADESSTABLE:
                return UpgradeSSTableTask.parse(info);
            case TEMPLATE:
                return CassandraTemplateTask.parse(info);
            default:
                throw new RuntimeException("Failed to parse task from TaskInfo " +
                    "type information is invalid");
        }
    }

    /**
     * Gets a unique identifier.
     *
     * @return A universally unique identifier.
     */
    public static Protos.TaskID createId(final String name) {
        return TaskUtils.toTaskId(name);
    }

    private final Protos.TaskInfo info;

    protected CassandraData getData() {
        return CassandraData.parse(info.getData());
    }

    protected Protos.TaskInfo.Builder getBuilder() {
        return Protos.TaskInfo.newBuilder(info);
    }

    public Protos.TaskStatus getCurrentStatus() {
        return getStatusBuilder()
            .setTaskId(info.getTaskId())
            .setData(getData().getBytes())
            .setState(getData()
                .getState()).build();
    }

    public Protos.TaskState getState(){
        return getData().getState();
    }


    /**
     * Constructs the base CassandraTask.
     */
    protected CassandraTask(final Protos.TaskInfo info) {
        this.info = info;
    }


    protected CassandraTask(
        final String name,
        final String configName,
        final CassandraTaskExecutor executor,
        final double cpus,
        final int memoryMb,
        final int diskMb,
        final VolumeRequirement.VolumeMode volumeMode,
        final VolumeRequirement.VolumeType volumeType,
        final Collection<Integer> ports,
        @Nullable final DiscoveryInfo discoveryInfo,
        final CassandraData data) {

        String role = executor.getRole();
        String principal = executor.getPrincipal();

        Protos.TaskInfo.Builder builder = Protos.TaskInfo.newBuilder()
            .setTaskId(createId(name))
            .setName(name)
            .setSlaveId(EMPTY_SLAVE_ID)
            .setExecutor(executor.getExecutorInfo())
            .addAllResources(Arrays.asList(
                ResourceUtils.getDesiredScalar(role, principal, "cpus", cpus),
                ResourceUtils.getDesiredScalar(role, principal, "mem", memoryMb)))
            .setData(data.getBytes());
        final Protos.Label label = Protos.Label.newBuilder()
                .setKey("config_target")
                .setValue(configName)
                .build();
        builder.setLabels(Protos.Labels.newBuilder().addLabels(label));

        if (!volumeMode.equals(VolumeRequirement.VolumeMode.NONE)) {
            if (volumeType.equals(VolumeRequirement.VolumeType.MOUNT)) {
                builder.addResources(ResourceUtils.getDesiredMountVolume(role, principal, diskMb, CassandraConfig.VOLUME_PATH));
            } else {
                builder.addResources(ResourceUtils.getDesiredRootVolume(role, principal, diskMb, CassandraConfig.VOLUME_PATH));
            }
        }

        if (!ports.isEmpty()) {
            builder.addResources(ResourceUtils.getDesiredRanges(role, principal, "ports", Algorithms.createRanges(ports)));
        }

        if (discoveryInfo != null) {
            builder.setDiscovery(discoveryInfo);
        }

        info = builder.build();
    }


    /**
     * Gets the unique identifier for the task.
     *
     * @return The universally unique identifier for the task.
     */
    public String getId() {
        return info.getTaskId().getValue();
    }

    /**
     * Gets the task name.
     *
     * @return The name of the task.
     */
    public String getName() {
        return info.getName();
    }

    /**
     * Gets the task's status.
     *
     * @return The status associated with the task.
     */
    public Protos.TaskStatus.Builder getStatusBuilder(
        final Protos.TaskState state,
        final Optional<String> message
    ) {
        Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder()
            .setSlaveId(info.getSlaveId())
            .setTaskId(info.getTaskId())
            .setState(state)
            .setData(getData().withState(state).getBytes())
            .setExecutorId(info.getExecutor().getExecutorId())
            .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR);
        message.map(builder::setMessage);
        return builder;
    }

    protected Protos.TaskStatus.Builder getStatusBuilder() {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(getTaskInfo().getTaskId())
                .setSlaveId(getTaskInfo().getSlaveId())
                .setExecutorId(getTaskInfo().getExecutor().getExecutorId())
                .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR);
    }

    /**
     * Gets the executor for the task.
     *
     * @return The CassandraTaskExecutor associated with the task.
     */
    public CassandraTaskExecutor getExecutor() {
        return new CassandraTaskExecutor(info.getExecutor());
    }

    /**
     * Gets the hostname of the task. This may be empty if the task has not
     * yet been launched.
     *
     * @return The hostname associated with the task.
     */
    public String getHostname() {
        return getData().getHostname();
    }

    /**
     * Gets the id of the slave the task is running on. This may be empty if the
     * task is not yet launched.
     *
     * @return The identifier of the slave the task is running on.
     */
    public String getSlaveId() {
        return info.getSlaveId().getValue();
    }

    /**
     * Gets the tasks type.
     *
     * @return The TYPE of the task.
     */
    public TYPE getType() {
        return getData().getType();
    }

    /**
     * Gets a Protocol Buffers representation of the task.
     *
     * @return A TaskInfo containing a Protocol Buffers representation of the
     * task.
     */
    public Protos.TaskInfo getTaskInfo() {
        return info;
    }

    /**
     * Tests if the task is terminated.
     *
     * @return True if the task is terminated.
     */
    public boolean isTerminated() {
        return CassandraTaskStatus.isTerminated(getData().getState());
    }

    /**
     * Tests if the task is running.
     *
     * @return True if the task is running.
     */
    public boolean isRunning() {
        return CassandraTaskStatus.isRunning(getData().getState());
    }

    /**
     * Tests if the task is launching.
     *
     * @return True if the task is launching.
     */
    public boolean isLaunching() {
        return CassandraTaskStatus.isLaunching(getData().getState());
    }

    /**
     * Updates the task with the provided offer.
     *
     * @param offer The Offer that will be used to launch the task.
     * @return A copy of the task with the hostname and slave id extracted
     * from offer. The task will be in the staging state.
     */
    public abstract CassandraTask update(Protos.Offer offer);

    /**
     * Updates the task id.
     *
     * @return A copy of the task with a new taskId
     */
    public abstract CassandraTask updateId();


    /**
     * Updates the tasks status.
     *
     * @param status The status that will be associated with the task.
     * @return A copy of the task with its status set to status.
     */
    public abstract CassandraTask update(CassandraTaskStatus status);

    /**
     * Updates the state of the task.
     *
     * @param state The TaskState associated with the tasks' status.
     * @return A copy of the task with its status's state set to state.
     */
    public abstract CassandraTask update(Protos.TaskState state);

    public abstract CassandraTaskStatus createStatus(
        final Protos.TaskState state,
        final Optional<String> message);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTask)) return false;
        CassandraTask that = (CassandraTask) o;
        return Objects.equals(info, that.info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(info);
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(info);
    }
}
