package com.mesosphere.dcos.cassandra.scheduler.config;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import io.dropwizard.lifecycle.Managed;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

public class ConfigurationManager implements Managed {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationManager.class);

    private final CassandraDaemonTask.Factory cassandraDaemonTaskFactory;
    private final DefaultConfigurationManager configurationManager;

    @Inject
    public ConfigurationManager(
            CassandraDaemonTask.Factory cassandraDaemonTaskFactory,
            DefaultConfigurationManager configurationManager) {
        this.cassandraDaemonTaskFactory = cassandraDaemonTaskFactory;
        this.configurationManager = configurationManager;
    }

    public CassandraTaskExecutor createExecutor(String frameworkId,
                                                String name,
                                                String role,
                                                String principal) throws ConfigStoreException {
        final ExecutorConfig executorConfig = getTargetConfig().getExecutorConfig();
        return CassandraTaskExecutor.create(
            frameworkId,
            name,
            role,
            principal,
            executorConfig);
    }

    public CassandraDaemonTask createDaemon(String frameworkId,
                                            String name,
                                            String role,
                                            String principal) {
        return CassandraDaemonTask.create(
            name,
            configName,
            createExecutor(frameworkId, name + "_executor", role, principal),
            cassandraConfig);
    }

    public CassandraDaemonTask moveDaemon(
            CassandraDaemonTask daemonTask,
            String frameworkId,
            String role,
            String principal) throws ConfigStoreException {
        CassandraTaskExecutor executor = createExecutor(frameworkId, daemonTask.getName() + "_executor", role, principal);
        return cassandraDaemonTaskFactory.move(daemonTask, executor);
    }

    public CassandraDaemonTask replaceDaemon(CassandraDaemonTask task) {
       return task.updateId();
    }


    public boolean hasCurrentConfig(final CassandraDaemonTask task) throws ConfigStoreException {
        final Optional<String> taskConfig = getTaskConfig(task);
        if (!taskConfig.isPresent()) {
            throw new RuntimeException("Invalid task. Should have a CONFIG_TARGET: " + task.getTaskInfo());
        }
        final String taskConfigName = taskConfig.get();
        final String targetConfigName = configurationManager.getTargetName().toString();
        LOGGER.info("TaskConfigName: {} TargetConfigName: {}", taskConfigName, targetConfigName);
        return targetConfigName.equals(taskConfigName);
    }

    private Optional<String> getTaskConfig(CassandraDaemonTask task) {
        final Protos.TaskInfo taskInfo = task.getTaskInfo();
        if (!taskInfo.hasLabels() || CollectionUtils.isEmpty(taskInfo.getLabels().getLabelsList())) {
            return Optional.empty();
        }

        for (Protos.Label label : taskInfo.getLabels().getLabelsList()) {
            final String key = label.getKey();
            if (PersistentOfferRequirementProvider.CONFIG_TARGET_KEY.equals(key)) {
                return Optional.ofNullable(label.getValue());
            }
        }

        return Optional.empty();
    }

    public CassandraDaemonTask updateConfig(final CassandraDaemonTask task) throws ConfigStoreException {
        CassandraConfig cassandraConfig = getTargetConfig().getCassandraConfig();
        return task.updateConfig(cassandraConfig, getTargetConfigName());
    }

    public CassandraSchedulerConfiguration getTargetConfig() throws ConfigStoreException {
        return ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig());
    }

    public UUID getTargetConfigName() throws ConfigStoreException {
        return configurationManager.getTargetName();
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
