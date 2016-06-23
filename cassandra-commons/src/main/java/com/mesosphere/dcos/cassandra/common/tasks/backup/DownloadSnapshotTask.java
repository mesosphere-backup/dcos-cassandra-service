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

import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.VolumeRequirement;

import java.util.Collections;
import java.util.Optional;

/**
 * DownloadSnapshotTask extends CassandraTask to implement a task that
 * downloads the snapshots of a set of key spaces and column families for a
 * Cassandra cluster to a node. The task can only be launched successfully if
 * the CassandraDaemonTask is running on the targeted slave.
 */
public class DownloadSnapshotTask extends CassandraTask {

    /**
     * Gets the name of a DownloadSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  DownloadSnapshotTaskfor daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return daemonName;
    }

    /**
     * Gets the name of a DownloadSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the  DownloadSnapshotTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    public static DownloadSnapshotTask parse(final Protos.TaskInfo info){
        return new DownloadSnapshotTask(info);
    }

    public static DownloadSnapshotTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final RestoreContext context) {

        String name = nameForDaemon(daemon);
        CassandraData data = CassandraData.createSnapshotDownloadData(
                "",
                context
                    .forNode(name)
                    .withLocalLocation(daemon.getVolumePath() + "/data"));

        Protos.TaskInfo completedTemplate = Protos.TaskInfo.newBuilder(template)
                .setName(name)
                .setData(data.getBytes())
                .build();

        completedTemplate = org.apache.mesos.offer.TaskUtils.clearTransient(completedTemplate);

        return new DownloadSnapshotTask(completedTemplate);
    }

    /**
     * Constructs a new DownloadSnapshotTask.
     */
    protected DownloadSnapshotTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public DownloadSnapshotTask update(Protos.Offer offer) {
        return new DownloadSnapshotTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public DownloadSnapshotTask updateId() {
        return new DownloadSnapshotTask(
            getBuilder().setTaskId(createId(getName()))
                .build());
    }

    @Override
    public DownloadSnapshotTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.SNAPSHOT_DOWNLOAD &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public DownloadSnapshotTask update(Protos.TaskState state) {
        return new DownloadSnapshotTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public DownloadSnapshotStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return DownloadSnapshotStatus.create(builder
                .setData(CassandraData.createSnapshotDownloadStatusData().getBytes())
                .setState(state)
                .build());
    }

    public RestoreContext getRestoreContext() {
        return getData().getRestoreContext();
    }
   
}
