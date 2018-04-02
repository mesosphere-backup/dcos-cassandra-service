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

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.DiscoveryInfo;
import org.apache.mesos.Protos.Port;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.offer.VolumeRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * CassandraDaemonTask extends CassandraTask to implement the task for a
 * Cassandra daemon. This is the task that starts and monitors the Cassandra
 * node. It is the first task launched on slave with a new executor, and it
 * must be running for additional tasks to run successfully on the slave. In
 * addition to the basic CassandraTask properties it contains the configuration
 * for the Cassandra node.
 */
public class CassandraDaemonTask extends CassandraTask {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CassandraDaemonTask.class);

    /**
     * String prefix for the CassandraDaemon task.
     */
    public static final String NAME_PREFIX = "node-";

    /**
     * Public node name used in VIP
     */
    public static final String VIP_NODE_NAME = "node";

    /**
     * Public port used in VIP
     */
    public static final int VIP_NODE_PORT = 9042; // cassandra's default native transport port


    public static CassandraDaemonTask parse(final Protos.TaskInfo info) {
        return new CassandraDaemonTask(info);
    }

    /**
     * Factory for {@link CassandraDaemonTask}s.
     */
    public static class Factory {

        private final Capabilities capabilities;

        @Inject
        public Factory(Capabilities capabilities) {
            this.capabilities = capabilities;
        }

        public CassandraDaemonTask create(
                final String name,
                final String configName,
                final CassandraTaskExecutor executor,
                final CassandraConfig config) {
            return new CassandraDaemonTask(name, configName, executor, config, capabilities);
        }

        public CassandraDaemonTask move(
                CassandraDaemonTask currentDaemon, CassandraTaskExecutor executor) {
            final List<Protos.Label> labelsList =
                    currentDaemon.getTaskInfo().getLabels().getLabelsList();
            String configName = "";
            for (Protos.Label label : labelsList) {
                if ("config_target".equals(label.getKey())) {
                    configName = label.getValue();
                }
            }
            if (StringUtils.isBlank(configName)) {
                throw new IllegalStateException("Task should have 'config_target'");
            }
            CassandraDaemonTask replacementDaemon = new CassandraDaemonTask(
                    currentDaemon.getName(),
                    configName,
                    executor,
                    currentDaemon.getConfig(),
                    capabilities,
                    currentDaemon.getData().replacing(currentDaemon.getData().getHostname()));

            Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder(replacementDaemon.getTaskInfo())
                    .setTaskId(currentDaemon.getTaskInfo().getTaskId())
                    .build();

            return new CassandraDaemonTask(taskInfo);
        }

        public CassandraDaemonTask create(
                final String name,
                final String configName,
                final CassandraTaskExecutor executor,
                final CassandraConfig config,
                final CassandraData data) {
            return new CassandraDaemonTask(name, configName, executor, config, capabilities, data);
        }
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
    private CassandraDaemonTask(
        final String name,
        final String configName,
        final CassandraTaskExecutor executor,
        final CassandraConfig config,
        final Capabilities capabilities,
        final CassandraData data) {
        super(
            name,
            configName,
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
            getDiscoveryInfo(
                    config.getPublishDiscoveryInfo(),
                    config.getApplication().getClusterName(),
                    capabilities,
                    name,
                    config.getApplication().getNativeTransportPort()),
            data);
    }

    private CassandraDaemonTask(
            final String name,
            final String configName,
            final CassandraTaskExecutor executor,
            final CassandraConfig config,
            final Capabilities capabilities) {
        this(
            name,
            configName,
            executor,
            config,
            capabilities,
            CassandraData.createDaemonData(
                "",
                CassandraMode.STARTING,
                config));
    }

    private CassandraDaemonTask(final Protos.TaskInfo info) {
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

    public CassandraDaemonTask updateConfig(CassandraConfig cassandraConfig,
                                            ExecutorConfig executorConfig,
                                            UUID targetConfigName) {
        LOGGER.info("Updating config for task: {} to config: {}", getTaskInfo().getName(), targetConfigName.toString());
        final Protos.Label label = Protos.Label.newBuilder()
                .setKey("config_target")
                .setValue(targetConfigName.toString())
                .build();
        return new CassandraDaemonTask(getBuilder()
            .setExecutor(getExecutor().update(executorConfig).getExecutorInfo())
            .setTaskId(createId(getName()))
            .setData(getData().withNewConfig(cassandraConfig).getBytes())
            .clearResources()
            .addAllResources(TaskUtils.updateResources(
                cassandraConfig.getCpus(),
                cassandraConfig.getMemoryMb(),
                getTaskInfo().getResourcesList()
            ))
            .clearLabels()
            .setLabels(Protos.Labels.newBuilder().addLabels(label).build()).build());
    }

    @Override
    public CassandraDaemonTask updateId() {
        return new CassandraDaemonTask(getBuilder()
            .setTaskId(createId(getName()))
            .setExecutor(getExecutor().clearId().getExecutorInfo())
            .setData(getData()
                .withState(Protos.TaskState.TASK_STAGING).getBytes())
            .build());
    }

    @Nullable
    private static DiscoveryInfo getDiscoveryInfo(boolean publishDiscoveryInfo,
                                                  String clusterName,
                                                  Capabilities capabilities,
                                                  String nodeName,
                                                  int nativePort) {

        // If the explicit configuration flag for publishing discovery info is set, include the cluster name in the
        // discovery info name and don't use labels.
        if (publishDiscoveryInfo) {
            DiscoveryInfo.Builder discoveryBuilder = DiscoveryInfo.newBuilder()
                    .setVisibility(DiscoveryInfo.Visibility.EXTERNAL)
                    .setName(clusterName + "." + nodeName);
            discoveryBuilder.getPortsBuilder().addPortsBuilder()
                    .setName("NativeTransport")
                    .setNumber(nativePort);
            return discoveryBuilder.build();
        }

        // Else, check if DC/OS has the right capabilities and publish the discovery info the DC/OS way.
        try {
            if (!capabilities.supportsNamedVips()) {
                return null;
            }
        } catch (Exception e) {
            LOGGER.warn("Unable to determine Named VIP support, assuming they are unavailable.", e);
            return null;
        }
        DiscoveryInfo.Builder discoveryBuilder = DiscoveryInfo.newBuilder()
            .setVisibility(DiscoveryInfo.Visibility.EXTERNAL)
            .setName(nodeName);
        Port.Builder portBuilder = discoveryBuilder.getPortsBuilder().addPortsBuilder()
            .setNumber(nativePort)
            .setProtocol("tcp");
        portBuilder.getLabelsBuilder().addLabelsBuilder()
            .setKey("VIP_" + UUID.randomUUID())
            .setValue(String.format("%s:%d", VIP_NODE_NAME, VIP_NODE_PORT));
        return discoveryBuilder.build();
    }
}
