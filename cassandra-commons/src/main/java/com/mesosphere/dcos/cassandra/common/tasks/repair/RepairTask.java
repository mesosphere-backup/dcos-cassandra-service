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
package com.mesosphere.dcos.cassandra.common.tasks.repair;


import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import org.apache.mesos.offer.VolumeRequirement;
import org.apache.mesos.Protos;

import java.util.Collections;
import java.util.Optional;

/**
 * RepairTask performs primary range, sequential, single data center
 * anti-entropy repair on a node. In order to successfully execute, a
 * CassandraDaemonTask must be running on the slave. If the indicated key
 * spaces are empty, all non-system key spaces will be repaired. If the column
 * families are empty, all column families for the selected key spaces will
 * be repaired.
 */
public class RepairTask extends CassandraTask {
    /**
     * Prefix for the name of RepairTasks
     */
    public static final String NAME_PREFIX = "repair-";


    /**
     * Gets the name of a RepairTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  RepairTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a RepairTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the  RepairTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    public static RepairTask parse(final Protos.TaskInfo info) {
        return new RepairTask(info);
    }

    public static RepairTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final RepairContext context) {

        String name = nameForDaemon(daemon);
        CassandraData data = CassandraData.createRepairData("", context);

        Protos.TaskInfo completedTemplate = Protos.TaskInfo.newBuilder(template)
                .setName(name)
                .setData(data.getBytes())
                .build();

        completedTemplate = org.apache.mesos.offer.TaskUtils.clearTransient(completedTemplate);

        return new RepairTask(completedTemplate);
    }

    protected RepairTask(final Protos.TaskInfo info) {
        super(info);
    }

    /**
     * Constructs a new RepairTask.
     */
    protected RepairTask(
        final String name,
        final CassandraTaskExecutor executor,
        final ClusterTaskConfig config,
        final RepairContext context) {
        super(name,
            executor,
            config.getCpus(),
            config.getMemoryMb(),
            config.getDiskMb(),
            VolumeRequirement.VolumeMode.NONE,
            null,
            Collections.emptyList(),
            CassandraData.createRepairData("", context));
    }

    @Override
    public RepairTask update(Protos.Offer offer) {
        return new RepairTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public RepairTask updateId() {
        return new RepairTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public RepairTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.REPAIR &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public RepairTask update(Protos.TaskState state) {
        return new RepairTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public RepairStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return RepairStatus.create(builder
                .setData(CassandraData.createRepairStatusData().getBytes())
                .setState(state)
                .build());
    }


    public RepairContext getRepairContext() {
        return getData().getRepairContext();
    }


}
