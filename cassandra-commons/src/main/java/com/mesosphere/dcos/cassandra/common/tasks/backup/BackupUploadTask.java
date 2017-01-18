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

import com.mesosphere.dcos.cassandra.common.tasks.*;

import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.Protos;

import java.util.Optional;

/**
 * BackupUploadTask extends CassandraTask to implement a task that
 * uploads the snapshots of a set of key spaces and column families for a
 * Cassandra cluster. The task can only be launched successfully if the
 * CassandraDaemonTask is running on the targeted slave and the
 * BackSnapshotTask has completed.
 * If the key spaces for the task are empty. All non-system key spaces are
 * backed up.
 * If the column families for the task are empty. All column families for the
 * indicated key spaces are backed up.
 */
public class BackupUploadTask extends CassandraTask {

    /**
     * The name prefix for BackupUploadTasks.
     */
    public static final String NAME_PREFIX = "upload-";

    /**
     * Gets the name of a BackupUploadTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the BackupUploadTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a BackupUploadTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the BackupUploadTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    public static BackupUploadTask parse(final Protos.TaskInfo info) {
        return new BackupUploadTask(info);
    }

    public static BackupUploadTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final BackupRestoreContext context) {

        CassandraData data = CassandraData.createBackupUploadData(
                "",
                context
                    .forNode(daemon.getName())
                    .withLocalLocation(daemon.getVolumePath() + "/data"));

        String name = nameForDaemon(daemon);
        Protos.TaskInfo completedTemplate = Protos.TaskInfo.newBuilder(template)
                .setName(name)
                .setTaskId(TaskUtils.toTaskId(name))
                .setData(data.getBytes())
                .build();

        completedTemplate = org.apache.mesos.offer.TaskUtils.clearTransient(completedTemplate);

        return new BackupUploadTask(completedTemplate);
    }

    /**
     * Constructs a new BackupUploadTask.
     */
    protected BackupUploadTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public BackupUploadTask update(Protos.Offer offer) {
        return new BackupUploadTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public BackupUploadTask updateId() {
        return new BackupUploadTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public BackupUploadTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.BACKUP_UPLOAD &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public BackupUploadTask update(Protos.TaskState state) {
        return new BackupUploadTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public BackupUploadStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return BackupUploadStatus.create(builder
                .setData(CassandraData.createBackupUploadStatusData().getBytes())
                .setState(state)
                .build());
    }


    public BackupRestoreContext getBackupRestoreContext() {
        return getData().getBackupRestoreContext();
    }
}