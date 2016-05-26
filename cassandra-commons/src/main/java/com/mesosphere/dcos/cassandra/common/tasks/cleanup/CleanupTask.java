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
package com.mesosphere.dcos.cassandra.common.tasks.cleanup;


import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import org.apache.mesos.Protos;

import java.util.Collections;
import java.util.Optional;

/**
 * CleanupTask extends CassandraTask to implement node cleanup. A
 * CassandraDaemonTask must be running on the slave for a CleanupTask to
 * successfully execute.
 * Cleanup removes all keys for a node that no longer fall in the token range
 * for the node. Cleanup should be run as a maintenance activity after node
 * addition, node removal, or node
 * replacement.
 * If the key spaces for the context are empty, all non-system key spaces are
 * used.
 * If the column families for the context are empty, all non-system column
 * families are used.
 */
public class CleanupTask extends CassandraTask {

    /**
     * The name prefix for a CleanupTask.
     */
    public static final String NAME_PREFIX = "cleanup-";

    /**
     * Gets the name of a CleanupTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  CleanupTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a CleanupTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the  CleanupTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    public static CleanupTask parse(final Protos.TaskInfo info) {
        return new CleanupTask(info);
    }

    public static CleanupTask create(final CassandraDaemonTask task,
                                     final ClusterTaskConfig config,
                                     final CleanupContext context) {
        return new CleanupTask(nameForDaemon(task),
            task.getExecutor(),
            config,
            context);
    }

    protected CleanupTask(final Protos.TaskInfo info) {
        super(info);
    }

    /**
     * Constructs a new CleanupTask.
     */
    protected CleanupTask(
        final String name,
        final CassandraTaskExecutor executor,
        final ClusterTaskConfig config,
        final CleanupContext context) {
        super(name,
            executor,
            config.getCpus(),
            config.getMemoryMb(),
            config.getDiskMb(),
            "",
            Collections.emptyList(),
            CassandraData.createCleanupData("", context));
    }

    @Override
    public CleanupTask update(Protos.Offer offer) {
        return new CleanupTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public CleanupTask updateId() {
        return new CleanupTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public CleanupTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.CLEANUP &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public CleanupTask update(Protos.TaskState state) {
        return new CleanupTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public CleanupStatus createStatus(Protos.TaskState state,
                                      Optional<String> message) {
        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }
        return CleanupStatus.create(builder
            .setData(
                CassandraData.createCleanupStatusData()
                    .getBytes())
            .build());
    }


    public CleanupContext getCleanupContext() {
        return getData().getCleanupContext();
    }


}
