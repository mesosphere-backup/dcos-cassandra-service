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

import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.VolumeRequirement;

import java.util.Arrays;
import java.util.Optional;

/**
 * CassandraDaemonTask extends CassandraTask to implement the task for a
 * Cassandra daemon. This is the task that starts and monitors the Cassandra
 * node. It is the first task launched on slave with a new executor, and it
 * must be running for additional tasks to run successfully on the slave. In
 * addition to the basic CassandraTask properties it contains the configuration
 * for the Cassandra node.
 */
public class CassandraDaemonTask extends CassandraTask {

    /**
     * String prefix for the CassandraDaemon task.
     */
    public static final String NAME_PREFIX = "node-";


    public static CassandraDaemonTask parse(final Protos.TaskInfo info) {
        return new CassandraDaemonTask(info);
    }

    public static CassandraDaemonTask create(
        final String name,
        final CassandraTaskExecutor executor,
        final CassandraConfig config) {
        return new CassandraDaemonTask(name, executor, config);
    }

    private static CassandraConfig updateConfig(
        final Protos.TaskState state,
        final CassandraConfig config) {
        if (Protos.TaskState.TASK_RUNNING.equals(state)) {
            return config.mutable().setReplaceIp("").build();
        } else {
            return config;
        }
    }

    /**
     * Constructs a new CassandraDaemonTask.
     *
     * @param name     The name of the Cassandra node.
     * @param executor The exeuctor for the CassandraDaemonTask.
     * @param config   The configuration for the Cassandra node.
     */
    protected CassandraDaemonTask(
        final String name,
        final CassandraTaskExecutor executor,
        final CassandraConfig config) {

        super(
            name,
            executor,
            config.getCpus(),
            config.getMemoryMb(),
            config.getDiskMb(),
            VolumeRequirement.VolumeMode.CREATE,
            config.getDiskType(),
            Arrays.asList(config.getJmxPort(),
                config.getApplication().getStoragePort(),
                config.getApplication().getSslStoragePort(),
                config.getApplication().getRpcPort(),
                config.getApplication().getNativeTransportPort()),
            CassandraData.createDaemonData(
                "",
                CassandraMode.STARTING,
                config));
    }

    protected CassandraDaemonTask(final Protos.TaskInfo info) {
        super(info);
    }

    /**
     * Gets the CassandraConfig for the Cassandra daemon.
     *
     * @return The configuration object for the Cassandra daemon.
     */
    public CassandraConfig getConfig() {
        return getData().getConfig();
    }

    public CassandraMode getMode() {
        return getData().getMode();
    }

    @Override
    public CassandraDaemonTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.CASSANDRA_DAEMON &&
            status.getId().equals(getId())) {
            CassandraDaemonStatus daemonStatus = (CassandraDaemonStatus) status;
            return new CassandraDaemonTask(
                Protos.TaskInfo.newBuilder(getTaskInfo())
                    .setData(getData().updateDaemon(
                        daemonStatus.getState(),
                        daemonStatus.getMode(),
                        updateConfig(daemonStatus.getState(), getConfig())
                    ).getBytes()).build()
            );
        }
        return this;

    }

    public VolumeRequirement.VolumeType getVolumeType() {
        return getConfig().getDiskType();
    }

    @Override
    public CassandraDaemonTask update(Protos.TaskState state) {
        return new CassandraDaemonTask(
            getBuilder()
                .setData(getData()
                    .withState(state)
                    .getBytes()).build());

    }

    public CassandraDaemonStatus createStatus(Protos.TaskState state,
                                              CassandraMode mode,
                                              Optional<String> message) {
        return CassandraDaemonStatus.create(getStatusBuilder(state, message)
            .setData(CassandraData.createDaemonStatusData(mode).getBytes())
            .build());
    }

    public String getVolumePath() {
        return TaskUtils.getVolumePaths(
            getTaskInfo().getResourcesList())
            .get(0);
    }

    @Override
    public CassandraDaemonStatus createStatus(Protos.TaskState state,
                                              Optional<String> message) {
        return createStatus(state, getMode(), message);
    }


    @Override
    public CassandraDaemonTask update(Protos.Offer offer) {
        return new CassandraDaemonTask(
            getBuilder()
                .setData(getData().withHostname(offer.getHostname()).getBytes())
                .setSlaveId(offer.getSlaveId())
                .build());
    }

    public CassandraDaemonTask updateConfig(CassandraConfig config) {
        return new CassandraDaemonTask(getBuilder()
            .setExecutor(getExecutor().withNewId().getExecutorInfo())
            .setTaskId(createId(getName()))
            .setData(getData().withNewConfig(config).getBytes())
            .clearResources()
            .addAllResources(TaskUtils.updateResources(
                config.getCpus(),
                config.getDiskMb(),
                getTaskInfo().getResourcesList()
            )).build());
    }

    public CassandraDaemonTask move() {
        return new CassandraDaemonTask(getBuilder()
            .setExecutor(getExecutor().withNewId().getExecutorInfo())
            .setTaskId(createId(getName()))
            .setSlaveId(EMPTY_SLAVE_ID)
            .setData(getData().replacing(getData().getHostname()).getBytes())
            .clearResources()
            .addAllResources(TaskUtils.replaceDisks(
                CassandraConfig.VOLUME_PATH,
                getTaskInfo().getResourcesList()
            )).build());
    }

    @Override
    public CassandraDaemonTask updateId() {
        return new CassandraDaemonTask(getBuilder()
            .setTaskId(createId(getName()))
            .setExecutor(getExecutor().withNewId().getExecutorInfo())
            .setData(getData()
                .withState(Protos.TaskState.TASK_STAGING).getBytes())
            .build());
    }


}
