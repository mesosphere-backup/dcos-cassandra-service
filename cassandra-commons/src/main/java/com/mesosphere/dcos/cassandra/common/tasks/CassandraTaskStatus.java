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
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupStatus;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairStatus;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * CassandraTaskStatus is the base type for the status object's associated with
 * CassandraTask's. There is a one to one correspondence between
 * CassandraTask types and CassandraTaskStatus types. When a new Task type is
 * added a status type should be added here and associated with the task type
 * . The type enumeration and parser of this class should be updated to
 * reflect the new status type.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = CassandraTaskStatus.class,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CassandraDaemonStatus.class, name =
                "CASSANDRA_DAEMON"),
        @JsonSubTypes.Type(value = BackupSnapshotStatus.class, name =
                "BACKUP_SNAPSHOT"),
        @JsonSubTypes.Type(value = BackupUploadStatus.class, name =
                "BACKUP_UPLOAD"),
        @JsonSubTypes.Type(value = DownloadSnapshotStatus.class, name =
                "SNAPSHOT_DOWNLOAD"),
        @JsonSubTypes.Type(value = RestoreSnapshotStatus.class, name =
                "SNAPSHOT_RESTORE"),
        @JsonSubTypes.Type(value = CleanupStatus.class, name =
                "CLEANUP"),
        @JsonSubTypes.Type(value = RepairStatus.class, name =
                "REPAIR"),
})
public abstract class CassandraTaskStatus {

    /**
     * Tests if the state is a terminal state.
     * @param state The state that will be tested.
     * @return True if state is a terminal state.
     */
    public static boolean isTerminated(final Protos.TaskState state) {
        switch (state) {
            case TASK_STARTING:
            case TASK_STAGING:
            case TASK_RUNNING:
                return false;
            default:
                return true;
        }
    }

    /**
     * Tests if the state is Running.
     * @param state The state to test.
     * @return True if the state is Running.
     */
    public static boolean isRunning(final Protos.TaskState state) {
        return Protos.TaskState.TASK_RUNNING.equals(state);
    }

    /**
     * Tests if the state is Launching.
     * @param state The state to test.
     * @return True if the state is Launching.
     */
    public static boolean isLaunching(final Protos.TaskState state) {
        switch (state) {
            case TASK_STARTING:
            case TASK_STAGING:
                return true;
            default:
                return false;
        }
    }

    /**
     * Tests if the state is Finished.
     * @param state The state to test.
     * @return True if the state is Finished.
     */
    public static boolean isFinished(final Protos.TaskState state) {
        return Protos.TaskState.TASK_FINISHED.equals(state);
    }

    /**
     * Parses a CassandraTaskStatus from a Protocol Buffers representation.
     * @param status The TaskStatus to parse.
     * @return A CassandraTaskStatus parsed from status.
     * @throws IOException If a CassandraTaskStatus could not be parsed from
     * status.
     */
    public static CassandraTaskStatus parse(Protos.TaskStatus status)
            throws IOException {
        CassandraProtos.CassandraTaskStatusData data =
                CassandraProtos.CassandraTaskStatusData.parseFrom(status
                        .getData());

        switch (data.getType()) {
            case CASSANDRA_DAEMON:
                return CassandraDaemonStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty(),
                        CassandraMode.values()[data.getMode()]);

            case BACKUP_SNAPSHOT:
                return BackupSnapshotStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty());

            case BACKUP_UPLOAD:
                return BackupUploadStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty());

            case SNAPSHOT_DOWNLOAD:
                return DownloadSnapshotStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty()
                );

            case SNAPSHOT_RESTORE:
                return RestoreSnapshotStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty()
                );

            case CLEANUP:
                return CleanupStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty()
                );

            case REPAIR:
                return RepairStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty()
                );

            default:
                return null;
        }

    }

    @JsonProperty("state")
    protected final Protos.TaskState state;
    @JsonProperty("type")
    protected final CassandraTask.TYPE type;
    @JsonProperty("slave_id")
    protected final String slaveId;
    @JsonProperty("id")
    protected final String id;
    @JsonProperty("executor_id")
    protected final String executorId;
    @JsonProperty("message")
    protected final Optional<String> message;

    /**
     * Constructs a base CassandraTaskStatus
     * @param type The TYPE of the CassandraTask associated with the status.
     * @param state The state of the associated task.
     * @param id The universally unique id of the associated task.
     * @param slaveId The id of the slave on which the task was launched.
     * @param executorId The id of the task's executor.
     * @param message An optional message for the status.
     */
    protected CassandraTaskStatus(
            CassandraTask.TYPE type,
            Protos.TaskState state,
            String id,
            String slaveId,
            String executorId,
            Optional<String> message) {
        this.type = type;
        this.state = state;
        this.id = id;
        this.slaveId = slaveId;
        this.executorId = executorId;
        this.message = message;
    }

    /**
     * Gets the state.
     * @return The state of the task associated with the status.
     */
    public Protos.TaskState getState() {
        return state;
    }

    /**
     * Gets the id.
     * @return The universally unique identifier of the task associated with
     * the status.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the executor id.
     * @return The id of the executor for the task associated with the status.
     */
    public String getExecutorId() {
        return executorId;
    }

    /**
     * Gets the status's message.
     * @return An optional message associated with the status.
     */
    public Optional<String> getMessage() {
        return message;
    }

    /**
     * Gets the slave id.
     * @return The id of the slave on which the task associated with the status
     * was launched.
     */
    public String getSlaveId() {
        return slaveId;
    }

    /**
     * Gets the TYPE.
     * @return The TYPE of task associated with the status.
     */
    public CassandraTask.TYPE getType() {
        return type;
    }

    /**
     * Updates the status's state.
     * @param state The status's state.
     * @return A copy of the status with its state set to state.
     */
    @JsonIgnore
    public abstract CassandraTaskStatus update(Protos.TaskState state);

    /**
     * Gets the Cassandra specific data associated with the status.
     * @return A Protocol Buffers representation of the Cassandra specific
     * data for the status.
     */
    @JsonIgnore
    protected abstract CassandraProtos.CassandraTaskStatusData getData();

    public Protos.TaskStatus toProto() {
        Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder()
                .setTaskId(
                        Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(
                        Protos.SlaveID.newBuilder().setValue(slaveId))
                .setExecutorId(
                        Protos.ExecutorID.newBuilder().setValue(executorId))
                .setData(getData().toByteString())
                .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
                .setState(state);

        message.map(builder::setMessage);

        return builder.build();

    }

    /**
     * Tests is the status is running.
     * @return True if the task associated with the status is Running.
     */
    @JsonIgnore
    public boolean isRunning() {
        return isRunning(state);
    }

    /**
     * Tests if the status is terminated.
     * @return True if the task associated with the status is in a terminal
     * state.
     */
    @JsonIgnore
    public boolean isTerminated() {
        return isTerminated(state);
    }

    /**
     * Tests is the status is launching.
     * @return True if the task associated with the status is Launching.
     */
    @JsonIgnore
    public boolean isLaunching() {
        return isLaunching(state);
    }

    /**
     * Tests if the task is finished.
     * @return True if the task associated with the status is Finished.
     */
    @JsonIgnore
    public boolean isFinished() {
        return isFinished(state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTaskStatus)) return false;
        CassandraTaskStatus that = (CassandraTaskStatus) o;
        return getState() == that.getState() &&
                getType() == that.getType() &&
                Objects.equals(getSlaveId(), that.getSlaveId()) &&
                Objects.equals(getId(), that.getId()) &&
                Objects.equals(getExecutorId(), that.getExecutorId()) &&
                Objects.equals(getMessage(), that.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getState(), getType(), getSlaveId(), getId(),
                getExecutorId(), getMessage());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
