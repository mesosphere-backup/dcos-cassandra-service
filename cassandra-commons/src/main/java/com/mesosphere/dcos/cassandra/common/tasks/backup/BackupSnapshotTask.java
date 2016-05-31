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
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.VolumeRequirement;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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


    public static BackupSnapshotTask parse(final Protos.TaskInfo info) {
        return new BackupSnapshotTask(info);
    }

    public static BackupSnapshotTask create(final CassandraDaemonTask task,
                                     final ClusterTaskConfig config,
                                     final BackupContext context) {
        return new BackupSnapshotTask(nameForDaemon(task),
            task.getExecutor(),
            config,
            context,
            task.getVolumePath() + "/data");
    }

    protected BackupSnapshotTask(final Protos.TaskInfo info) {
        super(info);
    }

    /**
     * Constructs a new BackupSnapshotTask.
     */
    protected BackupSnapshotTask(
        final String name,
        final CassandraTaskExecutor executor,
        final ClusterTaskConfig config,
        final BackupContext context,
        final String localLocation) {
        super(name,
            executor,
            config.getCpus(),
            config.getMemoryMb(),
            config.getDiskMb(),
            VolumeRequirement.VolumeMode.NONE,
            null,
            Collections.emptyList(),
            CassandraData.createBackupSnapshotData("",
                context.forNode(name)
                    .withLocalLocation(localLocation)));
    }

    @Override
    public BackupSnapshotTask update(Protos.Offer offer) {
        return new BackupSnapshotTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public BackupSnapshotTask updateId() {
        return new BackupSnapshotTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public BackupSnapshotTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.BACKUP_SNAPSHOT &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public BackupSnapshotTask update(Protos.TaskState state) {
        return new BackupSnapshotTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public BackupSnapshotStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return BackupSnapshotStatus.create(builder
                .setData(CassandraData.createBackupSnapshotStatusData().getBytes())
                .setState(state)
                .build());
    }


    public BackupContext getBackupContext() {
        return getData().getBackupContext();
    }

}
