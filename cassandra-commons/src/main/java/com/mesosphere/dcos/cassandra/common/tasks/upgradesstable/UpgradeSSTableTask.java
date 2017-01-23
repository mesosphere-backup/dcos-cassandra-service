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
package com.mesosphere.dcos.cassandra.common.tasks.upgradesstable;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskUtils;

import java.util.Optional;

/**
 * UpgradeSSTableTask extends CassandraTask to implement upgrade SSTables. A
 * CassandraDaemonTask must be running on the slave for a UpgradeSSTableTask to
 * successfully execute.
 * UpgradeSSTable rewrites older SSTables to the current version of Cassandra.
 * If the key spaces for the context are empty, all non-system key spaces are
 * used.
 * If the column families for the context are empty, all non-system column
 * families are used.
 */
public class UpgradeSSTableTask extends CassandraTask {

    /**
     * The name prefix for a UpgradeSSTableTask.
     */
    public static final String NAME_PREFIX = "upgradesstable-";

    /**
     * Gets the name of a UpgradeSSTableTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  UpgradeSSTableTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a UpgradeSSTableTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the  UpgradeSSTableTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    public static UpgradeSSTableTask parse(final Protos.TaskInfo info) {
        return new UpgradeSSTableTask(info);
    }

    public static UpgradeSSTableTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final UpgradeSSTableContext context) {

        CassandraData data = CassandraData.createUpgradeSSTableData("", context);

        String name = nameForDaemon(daemon);
        Protos.TaskInfo completedTemplate = Protos.TaskInfo.newBuilder(template)
                .setName(name)
                .setTaskId(TaskUtils.toTaskId(name))
                .setData(data.getBytes())
                .build();

        completedTemplate = TaskUtils.clearTransient(completedTemplate);

        return new UpgradeSSTableTask(completedTemplate);
    }

    /**
     * Constructs a new UpgradeSSTableTask.
     */
    protected UpgradeSSTableTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public UpgradeSSTableTask update(Protos.Offer offer) {
        return new UpgradeSSTableTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public UpgradeSSTableTask updateId() {
        return new UpgradeSSTableTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public UpgradeSSTableTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.UPGRADESSTABLE &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public UpgradeSSTableTask update(Protos.TaskState state) {
        return new UpgradeSSTableTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public UpgradeSSTableStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return UpgradeSSTableStatus.create(builder
            .setData(CassandraData.createUpgradeSSTableStatusData().getBytes())
            .setState(state)
            .build());
    }

    public UpgradeSSTableContext getUpgradeSSTableContext() {
        return getData().getUpgradeSSTableContext();
    }

}
