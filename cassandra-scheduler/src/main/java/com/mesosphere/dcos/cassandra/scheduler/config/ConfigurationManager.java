package com.mesosphere.dcos.cassandra.scheduler.config;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.config.ConfigStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationManager implements Managed {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(ConfigurationManager.class);

    private final DefaultConfigurationManager configurationManager;

    @Inject
    public ConfigurationManager(
        DefaultConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public CassandraTaskExecutor updateExecutor(
        final CassandraTask task) throws ConfigStoreException {
        final ExecutorConfig executorConfig = ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                .getExecutorConfig();
        return task.getExecutor().matches(executorConfig) ?
            task.getExecutor() :
            task.getExecutor().update(executorConfig);
    }

    public CassandraTaskExecutor createExecutor(String frameworkId,
                                                String name,
                                                String role,
                                                String principal) throws ConfigStoreException {
        final ExecutorConfig executorConfig = ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                .getExecutorConfig();
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
                                            String principal) throws ConfigStoreException {
        final CassandraSchedulerConfiguration targetConfig = ((CassandraSchedulerConfiguration)configurationManager
                .getTargetConfig());
        final CassandraConfig cassandraConfig = targetConfig.getCassandraConfig();
        return CassandraDaemonTask.create(
            name,
            createExecutor(frameworkId, name + "_executor", role, principal),
            cassandraConfig.mutable().setApplication(cassandraConfig
                .getApplication()
                .toBuilder().setSeedProvider(
                    CassandraApplicationConfig
                        .createDcosSeedProvider(
                            targetConfig.getSeedsUrl()))
                .build())
                .build());
    }

    public CassandraDaemonTask moveDaemon(
            CassandraDaemonTask daemonTask,
            String frameworkId,
            String role,
            String principal) throws ConfigStoreException {
        CassandraTaskExecutor executor = createExecutor(frameworkId, daemonTask.getName() + "_executor", role, principal);
        return daemonTask.move(executor);
    }

    public CassandraDaemonTask replaceDaemon(CassandraDaemonTask task) {
       return task.updateId();
    }


    public boolean hasCurrentConfig(final CassandraDaemonTask task) throws ConfigStoreException {
        CassandraSchedulerConfiguration targetConfig = ((CassandraSchedulerConfiguration)configurationManager
                .getTargetConfig());
        final CassandraConfig cassandraConfig = targetConfig.getCassandraConfig();
        final ExecutorConfig executorConfig = targetConfig.getExecutorConfig();
        LOGGER.info("ExecutorConfig: " + executorConfig);
        LOGGER.info("CassandraConfig: " + cassandraConfig);
        LOGGER.info("Task: ", task);

        boolean executorMatches = task.getExecutor().matches(executorConfig);
        boolean cassandraConfigMatches = task.getConfig().equals(cassandraConfig);

        LOGGER.info("Executor  matches: " + executorMatches);
        LOGGER.info("Cassandra matches: " + cassandraConfigMatches);

        return executorMatches && cassandraConfigMatches;
    }

    public CassandraDaemonTask updateConfig(final CassandraDaemonTask task) throws ConfigStoreException {
        CassandraConfig cassandraConfig = ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                .getCassandraConfig();
        return task.updateConfig(cassandraConfig);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
