package com.mesosphere.dcos.cassandra.scheduler.config;


import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentReference;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class ConfigurationManager implements Managed {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(ConfigurationManager.class);

    private final PersistentReference<CassandraConfig> cassandraRef;
    private final PersistentReference<ClusterTaskConfig> clusterTaskRef;
    private final PersistentReference<ExecutorConfig> executorRef;
    private final PersistentReference<Integer> serversRef;
    private final PersistentReference<Integer> seedsRef;
    private volatile CassandraConfig cassandraConfig;
    private volatile ClusterTaskConfig clusterTaskConfig;
    private volatile ExecutorConfig executorConfig;
    private volatile int servers;
    private volatile int seeds;
    private final String placementStrategy;
    private final String seedsUrl;
    private final List<String> errors = new ArrayList<>();
    private final String dataCenterUrl;
    private final List<String> externalDataCenters;
    private final long dataCenterSyncDelayMs;

    private void reconcileConfiguration() throws PersistenceException {
        LOGGER.info("Reconciling remote and persisted configuration");

        Optional<Integer> serversOption = serversRef.load();

        if (serversOption.isPresent()) {
            int servers = serversOption.get();
            if (this.servers < servers) {
                String error = String.format("The number of configured " +
                        "nodes (%d) is less than the previous " +
                        "number of configured nodes (%d).",
                    this.servers,
                    servers);
                this.servers = servers;
                LOGGER.error(error);
                errors.add(error);
                return;
            }

        }

        if (seeds > servers) {
            String error = String.format("The number of configured " +
                    "seeds (%d) is greater than the current number " +
                    "of configured nodes (%d). Reduce the " +
                    "number of seeds or increase the number of nodes",
                seeds,
                servers);
            LOGGER.error(error);
            errors.add(error);
            return;
        }

        Optional<CassandraConfig> configOption = cassandraRef.load();


        if (configOption.isPresent()) {
            LOGGER.info("Loaded persistent configuration");

            CassandraConfig persistedConfig = configOption.get();


            if (!persistedConfig.getDiskType().equals(
                cassandraConfig.getDiskType())) {

                String error = String.format("The configured disk type can " +
                        "not be changed. Persisted disk type is (%s). " +
                        "Configured disk type is (%s)",
                    persistedConfig.getDiskType(),
                    cassandraConfig.getDiskType());

                LOGGER.error(error);
                errors.add(error);
                cassandraConfig = persistedConfig;
                return;
            }

            if (persistedConfig.getDiskMb() != cassandraConfig.getDiskMb()) {
                String error = String.format("The configured disk size can " +
                        "not be changed. Persisted disk size is " +
                        "(%d) Mb. Configured disk size is (%d) Mb",
                    persistedConfig.getDiskMb(),
                    cassandraConfig.getDiskMb());

                LOGGER.error(error);
                errors.add(error);
                cassandraConfig = persistedConfig;
                return;
            }

            if (persistedConfig.getDiskMb()
                != cassandraConfig.getDiskMb()) {
                String error = String.format("The configured volume size can" +
                        " not be changed. Persisted volume size is " +
                        "(%d) Mb. Configured volume size is (%d) Mb",
                    persistedConfig.getDiskMb(),
                    cassandraConfig.getDiskMb());

                LOGGER.error(error);
                errors.add(error);
                cassandraConfig = persistedConfig;
                return;
            }
        }

        serversRef.store(servers);
        seedsRef.store(seeds);
        cassandraRef.store(cassandraConfig);
        executorRef.store(executorConfig);


    }

    @Inject
    public ConfigurationManager(
        @Named("ConfiguredCassandraConfig") CassandraConfig cassandraConfig,
        @Named("ConfiguredClusterTaskConfig") ClusterTaskConfig clusterTaskConfig,
        @Named("ConfiguredExecutorConfig") ExecutorConfig executorConfig,
        @Named("ConfiguredServers") int servers,
        @Named("ConfiguredSeeds") int seeds,
        @Named("ConfiguredPlacementStrategy") String placementStrategy,
        @Named("SeedsUrl") String seedsUrl,
        @Named("ConfiguredDcUrl") String dataCenterUrl,
        @Named("ConfiguredExternalDcs") List<String> externalDataCenters,
        @Named("ConfiguredSyncDelayMs") final long dataCenterSyncDelayMs,
        PersistenceFactory persistenceFactory,
        Serializer<CassandraConfig> cassandraConfigSerializer,
        Serializer<ExecutorConfig> executorConfigSerializer,
        Serializer<ClusterTaskConfig> clusterTaskConfigSerializer,
        Serializer<Integer> intSerializer) {
        this.cassandraRef = persistenceFactory.createReference(
            "cassandraConfig",
            cassandraConfigSerializer);
        this.clusterTaskRef = persistenceFactory.createReference(
            "clusterTaskConfig",
            clusterTaskConfigSerializer
        );
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
        this.clusterTaskConfig = clusterTaskConfig;
        this.executorConfig = executorConfig;
        this.servers = servers;
        this.seeds = seeds;
        this.placementStrategy = placementStrategy;
        this.seedsUrl = seedsUrl;
        this.dataCenterUrl = dataCenterUrl;
        this.dataCenterSyncDelayMs = dataCenterSyncDelayMs;
        this.externalDataCenters = ImmutableList.copyOf(externalDataCenters);

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
        final CassandraTask task) {
        return task.getExecutor().matches(getExecutorConfig()) ?
            task.getExecutor() :
            task.getExecutor().update(getExecutorConfig());

    }

    public CassandraTaskExecutor createExecutor(String frameworkId,
                                                String name,
                                                String role,
                                                String principal) {
        return CassandraTaskExecutor.create(
            frameworkId,
            name,
            role,
            principal,
            getExecutorConfig());
    }

    public CassandraDaemonTask createDaemon(String frameworkId,
                                            String name,
                                            String role,
                                            String principal) {


        return CassandraDaemonTask.create(
            name,
            createExecutor(name + "_executor", frameworkId, role, principal),
            cassandraConfig.mutable().setApplication(cassandraConfig
                .getApplication()
                .toBuilder().setSeedProvider(
                    CassandraApplicationConfig
                        .createDcosSeedProvider(
                            seedsUrl))
                .build())
                .build());
    }

    public CassandraDaemonTask moveDaemon(CassandraDaemonTask daemonTask) {
        return daemonTask.move();
    }

    public BackupSnapshotTask createBackupSnapshotTask(
        CassandraDaemonTask daemon,
        BackupContext context) {
        return BackupSnapshotTask.create(daemon, clusterTaskConfig, context);
    }

    public DownloadSnapshotTask createDownloadSnapshotTask(
        CassandraDaemonTask daemon,
        RestoreContext context) {
        return DownloadSnapshotTask.create(daemon, clusterTaskConfig, context);

    }

    public RestoreSnapshotTask createRestoreSnapshotTask(
        CassandraDaemonTask daemon,
        RestoreContext context) {
        return RestoreSnapshotTask.create(daemon, clusterTaskConfig, context);
    }

    public BackupUploadTask createBackupUploadTask(
        CassandraDaemonTask daemon,
        BackupContext context) {
        return BackupUploadTask.create(daemon, clusterTaskConfig, context);
    }

    public CleanupTask createCleanupTask(
        CassandraDaemonTask daemon,
        CleanupContext context) {
        return CleanupTask.create(daemon, clusterTaskConfig, context);
    }

    public RepairTask createRepairTask(
        CassandraDaemonTask daemon,
        RepairContext context) {
        return RepairTask.create(daemon,
            clusterTaskConfig,
            context);
    }

    public CassandraDaemonTask replaceDaemon(CassandraDaemonTask task) {
       return task.updateId();
    }


    public boolean hasCurrentConfig(final CassandraDaemonTask task) {
        LOGGER.info("ExecutorConfig: " + executorConfig);
        LOGGER.info("CassandraConfig: " + cassandraConfig);
        LOGGER.info("Task: ", task);

        boolean executorMatches = task.getExecutor().matches(executorConfig);
        boolean cassandraConfigMatches = task.getConfig().equals(cassandraConfig);

        LOGGER.info("Executor  matches: " + executorMatches);
        LOGGER.info("Cassandra matches: " + cassandraConfigMatches);

        return executorMatches && cassandraConfigMatches;

        //return task.getExecutor().matches(executorConfig) &&
        //    task.getConfig().equals(cassandraConfig);
    }

    public CassandraDaemonTask updateConfig(final CassandraDaemonTask task) {
        return task.updateConfig(cassandraConfig);
    }

    public CassandraTask updateId(CassandraTask task) {
        return task.updateId();
    }

    public List<String> getErrors() {
        return ImmutableList.copyOf(errors);
    }

    public String getDataCenterUrl() {
        return dataCenterUrl;
    }

    public long getDataCenterSyncDelayMs() {
        return dataCenterSyncDelayMs;
    }

    public List<String> getExternalDataCenters() {
        return externalDataCenters;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
