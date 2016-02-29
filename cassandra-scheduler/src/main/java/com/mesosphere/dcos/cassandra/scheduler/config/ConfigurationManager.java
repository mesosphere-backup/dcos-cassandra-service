package com.mesosphere.dcos.cassandra.scheduler.config;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentReference;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;


public class ConfigurationManager implements Managed {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConfigurationManager.class);

    private final PersistentReference<CassandraConfig> cassandraRef;

    private final PersistentReference<ExecutorConfig> executorRef;
    private final PersistentReference<Integer> serversRef;
    private final PersistentReference<Integer> seedsRef;
    private volatile CassandraConfig cassandraConfig;
    private volatile ExecutorConfig executorConfig;
    private volatile int servers;
    private volatile int seeds;
    private volatile boolean updateConfig;
    private final String placementStrategy;
    private final String planStrategy;
    private final String seedsUrl;

    private void reconcileConfiguration() throws PersistenceException {
        LOGGER.info("Reconciling remote and persisted configuration");

        Optional<Integer> serversOption = serversRef.load();

        if (updateConfig) {
            LOGGER.info("Configuration update requested");

            if (serversOption.isPresent()) {
                int servers = serversOption.get();
                if (this.servers < servers) {
                    String error = String.format("The number of configured " +
                                    "servers (%d) is less than the current " +
                                    "number of configured servers (%d). Reduce the " +
                                    "number of servers by removing them from the cluster",
                            this.servers,
                            servers);
                    LOGGER.error(error);
                    throw new IllegalStateException(error);
                }

            }

            if (seeds > servers) {
                String error = String.format("The number of configured " +
                                "seeds (%d) is greater than the current number " +
                                "of configured servers (%d). Reduce the " +
                                "number of seeds or increase the number of servers",
                        seeds,
                        servers);
                LOGGER.error(error);
                throw new IllegalStateException(error);
            }

            serversRef.store(servers);
            seedsRef.store(seeds);
            cassandraRef.store(cassandraConfig);
            executorRef.store(executorConfig);

        } else {
            LOGGER.info("Using persisted configuration");
            servers = serversRef.putIfAbsent(servers);
            seeds = seedsRef.putIfAbsent(seeds);
            cassandraConfig = cassandraRef.putIfAbsent(cassandraConfig);
            executorConfig = executorRef.putIfAbsent(executorConfig);
        }
    }

    @Inject
    public ConfigurationManager(
            @Named("ConfiguredCassandraConfig") CassandraConfig cassandraConfig,
            @Named("ConfiguredExecutorConfig") ExecutorConfig executorConfig,
            @Named("ConfiguredServers") int servers,
            @Named("ConfiguredSeeds") int seeds,
            @Named("ConfiguredUpdateConfig") boolean updateConfig,
            @Named("ConfiguredPlacementStrategy") String placementStrategy,
            @Named("ConfiguredPlanStrategy") String planStrategy,
            @Named("SeedsUrl") String seedsUrl,
            PersistenceFactory persistenceFactory,
            Serializer<CassandraConfig> cassandraConfigSerializer,
            Serializer<ExecutorConfig> executorConfigSerializer,
            Serializer<Integer> intSerializer) {
        this.cassandraRef = persistenceFactory.createReference(
                "cassandraConfig",
                cassandraConfigSerializer);
        this.executorRef = persistenceFactory.createReference(
                "executorConfig",
                executorConfigSerializer);
        this.serversRef = persistenceFactory.createReference(
                "servers",
                intSerializer);
        this.seedsRef = persistenceFactory.createReference(
                "seeds",
                intSerializer);
        this.cassandraConfig = cassandraConfig;
        this.executorConfig = executorConfig;
        this.servers = servers;
        this.seeds = seeds;
        this.updateConfig = updateConfig;
        this.placementStrategy = placementStrategy;
        this.planStrategy = planStrategy;
        this.seedsUrl = seedsUrl;

        try {
            reconcileConfiguration();
        } catch (Throwable throwable) {
            throw new IllegalStateException("Failed to reconcile " +
                    "configuration",
                    throwable);
        }
    }

    public CassandraConfig getCassandraConfig() {

        return cassandraConfig;
    }

    public ExecutorConfig getExecutorConfig() {

        return executorConfig;
    }

    public int getServers() {
        return servers;
    }

    public int getSeeds() {
        return seeds;
    }

    public String getPlacementStrategy() {
        return placementStrategy;
    }

    public String getPlanStrategy() {
        return planStrategy;
    }

    public void setCassandraConfig(final CassandraConfig cassandraConfig)
            throws PersistenceException {

        synchronized (cassandraRef) {
            cassandraRef.store(cassandraConfig);
            this.cassandraConfig = cassandraConfig;
        }
    }

    public void setExecutorConfig(ExecutorConfig executorConfig)
            throws PersistenceException {

        synchronized (executorRef) {
            executorRef.store(executorConfig);
            this.executorConfig = executorConfig;
        }
    }

    public void setSeeds(int seeds) throws PersistenceException {

        synchronized (seedsRef) {
            seedsRef.store(seeds);
            this.seeds = seeds;
        }
    }

    public void setServers(int servers) throws PersistenceException {

        synchronized (serversRef) {
            serversRef.store(servers);
            this.servers = servers;
        }
    }

    public CassandraTaskExecutor updateExecutor(
            CassandraTask task,
            String newId) {

        return hasCurrentExecutorConfig(task.getExecutor()) ?
                task.getExecutor() :
                createExecutor(task.getExecutor().getFrameworkId(),
                        newId + "_executor");

    }

    public CassandraTaskExecutor createExecutor(String frameworkId,
                                                String id) {

        return CassandraTaskExecutor.create(
                frameworkId,
                id,
                executorConfig.getCommand(),
                executorConfig.getArguments(),
                executorConfig.getCpus(),
                executorConfig.getMemoryMb(),
                executorConfig.getDiskMb(),
                executorConfig.getHeapMb(),
                executorConfig.getApiPort(),
                executorConfig.getAdminPort(),
                Arrays.asList(executorConfig.getJreLocation(),
                        executorConfig.getExecutorLocation(),
                        executorConfig.getCassandraLocation()),
                executorConfig.getJavaHome()
        );
    }

    public CassandraDaemonTask createDaemon(String frameworkId,
                                            String slaveId,
                                            String hostname,
                                            String name,
                                            String role,
                                            String principal) {

        String unique = UUID.randomUUID().toString();

        String id = name + "_" + unique;

        String executor = name + "_" + unique + "_executor";

        return CassandraDaemonTask.create(
                id,
                slaveId,
                hostname,
                createExecutor(frameworkId, executor),
                name,
                role,
                principal,
                cassandraConfig.getCpus(),
                cassandraConfig.getMemoryMb(),
                cassandraConfig.getDiskMb(),
                cassandraConfig.mutable().setVolume(
                        cassandraConfig.getVolume().withId()).
                        setApplication(cassandraConfig.getApplication()
                                .toBuilder().setSeedProvider(
                                        CassandraApplicationConfig
                                                .createDcosSeedProvider(
                                                        seedsUrl))
                                .build())
                        .build(),
                CassandraDaemonStatus.create(Protos.TaskState.TASK_STAGING,
                        id,
                        slaveId,
                        name,
                        Optional.empty(),
                        CassandraMode.STARTING)

        );
    }

    public CassandraDaemonTask replaceDaemon(CassandraDaemonTask task) {
        String id = task.getName() + "_" + UUID.randomUUID().toString();
        return task.mutable()
                .setId(id)
                .setStatus(
                        CassandraDaemonStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                id,
                                task.getSlaveId(),
                                task.getName(),
                                Optional.empty(),
                                CassandraMode.STARTING)).build();

    }

    public boolean hasCurrentExecutorConfig(
            final CassandraTaskExecutor executor) {
       return executor.getCommand().equals(executorConfig
                .getCommand()) &&
                Double.compare(executor.getCpus(),
                        executorConfig.getCpus()) == 0 &&
               executor.getDiskMb() ==
                        executorConfig.getDiskMb() &&
               executor.getMemoryMb() ==
                        executorConfig.getMemoryMb() &&
               executor.getUriStrings().containsAll(
                        Arrays.asList(
                                executorConfig.getCassandraLocationString(),
                                executorConfig.getExecutorLocationString(),
                                executorConfig.getJreLocationString()
                        )
                ) &&
               executor.getHeapMb() == executorConfig.getHeapMb();
    }

    public boolean hasCurrentConfig(final CassandraDaemonTask task) {

        return  hasCurrentExecutorConfig(task.getExecutor()) &&
                task.getConfig().equals(cassandraConfig);

    }

    public CassandraDaemonTask updateConfig(final CassandraDaemonTask task) {

        String id = task.getName() + "_" + UUID.randomUUID().toString();
        return CassandraDaemonTask.create(
                id,
                task.getSlaveId(),
                task.getHostname(),
                updateExecutor(task,id),
                task.getName(),
                task.getRole(),
                task.getPrincipal(),
                cassandraConfig.getCpus(),
                cassandraConfig.getMemoryMb(),
                cassandraConfig.getDiskMb(),
                cassandraConfig.mutable().setVolume(
                        task.getConfig().getVolume()).
                        setApplication(cassandraConfig.getApplication()
                                .toBuilder().setSeedProvider(
                                        CassandraApplicationConfig
                                                .createDcosSeedProvider(
                                                        seedsUrl))
                                .build())
                        .build(),
                CassandraDaemonStatus.create(Protos.TaskState.TASK_STAGING,
                        id,
                        task.getSlaveId(),
                        task.getName(),
                        Optional.empty(),
                        CassandraMode.STARTING));
    }

    public CassandraTask updateId(CassandraTask task) {
        return task.updateId(
                task.getName() + "_" + UUID.randomUUID().toString());
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
