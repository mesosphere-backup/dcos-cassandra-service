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
import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskUtils;

import java.util.Optional;

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

    public static RestoreSnapshotTask parse(final Protos.TaskInfo info){
        return new RestoreSnapshotTask(info);
    }

    public static RestoreSnapshotTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final BackupRestoreContext context) {

        CassandraData data = CassandraData.createRestoreSnapshotData(
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

        return new RestoreSnapshotTask(completedTemplate);
    }

    /**
     * Constructs a new RestoreSnapshotTask.
     */
    protected RestoreSnapshotTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public RestoreSnapshotTask update(Protos.Offer offer) {
        return new RestoreSnapshotTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public RestoreSnapshotTask updateId() {
        return new RestoreSnapshotTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public RestoreSnapshotTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.SNAPSHOT_RESTORE &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public RestoreSnapshotTask update(Protos.TaskState state) {
        return new RestoreSnapshotTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public RestoreSnapshotStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return RestoreSnapshotStatus.create(builder
                .setData(CassandraData.createRestoreSnapshotStatusData().getBytes())
                .setState(state)
                .build());
    }

    public BackupRestoreContext getBackupRestoreContext() {
        return getData().getBackupRestoreContext();
    }
}