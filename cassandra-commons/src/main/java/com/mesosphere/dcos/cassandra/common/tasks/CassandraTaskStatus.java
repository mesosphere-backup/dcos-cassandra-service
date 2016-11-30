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
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupStatus;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairStatus;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableStatus;
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
public abstract class CassandraTaskStatus {

    /**
     * Tests if the state is a terminal state.
     *
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
     *
     * @param state The state to test.
     * @return True if the state is Running.
     */
    public static boolean isRunning(final Protos.TaskState state) {
        return Protos.TaskState.TASK_RUNNING.equals(state);
    }

    /**
     * Tests if the state is Launching.
     *
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
     *
     * @param state The state to test.
     * @return True if the state is Finished.
     */
    public static boolean isFinished(final Protos.TaskState state) {
        return Protos.TaskState.TASK_FINISHED.equals(state);
    }

    public static CassandraTaskStatus parse(final Protos.TaskStatus status)
        throws IOException {
        CassandraData data = CassandraData.parse(status.getData());
        switch (data.getType()) {
            case CASSANDRA_DAEMON:
                return CassandraDaemonStatus.create(status);
            case BACKUP_SNAPSHOT:
                return BackupSnapshotStatus.create(status);
            case BACKUP_SCHEMA:
                return BackupSchemaStatus.create(status);
            case BACKUP_UPLOAD:
                return BackupUploadStatus.create(status);
            case SNAPSHOT_DOWNLOAD:
                return DownloadSnapshotStatus.create(status);
            case SNAPSHOT_RESTORE:
                return RestoreSnapshotStatus.create(status);
            case SCHEMA_RESTORE:
                return RestoreSchemaStatus.create(status);
            case CLEANUP:
                return CleanupStatus.create(status);
            case REPAIR:
                return RepairStatus.create(status);
            case UPGRADESSTABLE:
                return UpgradeSSTableStatus.create(status);
            default:
                throw new IOException("Failed to parse task from TaskInfo " +
                    "type information is invalid");
        }
    }

    private final Protos.TaskStatus status;

    protected CassandraTaskStatus(final Protos.TaskStatus status) {
        this.status = status;
    }

    protected CassandraData getData() {
        return CassandraData.parse(status.getData());
    }

    /**
     * Gets the state.
     *
     * @return The state of the task associated with the status.
     */
    public Protos.TaskState getState() {
        return status.getState();
    }

    /**
     * Gets the id.
     *
     * @return The universally unique identifier of the task associated with
     * the status.
     */
    public String getId() {
        return status.getTaskId().getValue();
    }

    /**
     * Gets the status's message.
     *
     * @return An optional message associated with the status.
     */
    public Optional<String> getMessage() {
        return status.hasMessage() ? Optional.of(status.getMessage()) :
            Optional.empty();
    }

    /**
     * Gets the slave id.
     *
     * @return The id of the slave on which the task associated with the status
     * was launched.
     */
    public String getSlaveId() {
        return status.getSlaveId().getValue();
    }

    /**
     * Gets the TYPE.
     *
     * @return The TYPE of task associated with the status.
     */
    public CassandraTask.TYPE getType() {
        return getData().getType();
    }


    public Protos.TaskStatus getTaskStatus() {
        return status;
    }

    /**
     * Tests is the status is running.
     *
     * @return True if the task associated with the status is Running.
     */
    public boolean isRunning() {
        return isRunning(getState());
    }

    /**
     * Tests if the status is terminated.
     *
     * @return True if the task associated with the status is in a terminal
     * state.
     */
    public boolean isTerminated() {
        return isTerminated(getState());
    }

    /**
     * Tests is the status is launching.
     *
     * @return True if the task associated with the status is Launching.
     */
    public boolean isLaunching() {
        return isLaunching(getState());
    }

    /**
     * Tests if the task is finished.
     *
     * @return True if the task associated with the status is Finished.
     */
    public boolean isFinished() {
        return isFinished(getState());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTaskStatus)) return false;
        CassandraTaskStatus that = (CassandraTaskStatus) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.status);
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(status);
    }
}
