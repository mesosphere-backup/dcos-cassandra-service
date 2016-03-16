package com.mesosphere.dcos.cassandra.scheduler.config;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupStatus;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentReference;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


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

    private void reconcileConfiguration() throws PersistenceException {
        LOGGER.info("Reconciling remote and persisted configuration");

        Optional<Integer> serversOption = serversRef.load();


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
                errors.add(error);
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
            errors.add(error);
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
                cassandraConfig.mutable()
                        .setVolume(
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

    public CassandraDaemonTask moveDaemon(CassandraDaemonTask daemonTask) {

        return CassandraDaemonTask.create(
                daemonTask.getId(),
                "",
                "",
                daemonTask.getExecutor(),
                daemonTask.getName(),
                daemonTask.getRole(),
                daemonTask.getPrincipal(),
                cassandraConfig.getCpus(),
                cassandraConfig.getMemoryMb(),
                cassandraConfig.getDiskMb(),
                cassandraConfig.mutable().setReplaceIp(daemonTask.getHostname())
                        .setVolume(
                                cassandraConfig.getVolume().withId()).
                        setApplication(cassandraConfig.getApplication()
                                .toBuilder().setSeedProvider(
                                        CassandraApplicationConfig
                                                .createDcosSeedProvider(
                                                        seedsUrl))
                                .build())
                        .build(),
                daemonTask.getStatus());


    }

    public BackupSnapshotTask createBackupSnapshotTask(
            CassandraDaemonTask daemon,
            BackupContext context) {
        String name = BackupSnapshotTask.nameForDaemon(daemon);
        String id = name + "_" + UUID.randomUUID().toString();

        return BackupSnapshotTask.create(
                id,
                daemon.getSlaveId(),
                daemon.getHostname(),
                daemon.getExecutor(),
                name,
                daemon.getRole(),
                daemon.getPrincipal(),
                clusterTaskConfig.getCpus(),
                clusterTaskConfig.getMemoryMb(),
                clusterTaskConfig.getDiskMb(),
                BackupSnapshotStatus.create(Protos.TaskState.TASK_STAGING,
                        id,
                        daemon.getSlaveId(),
                        name,
                        Optional.empty()),
                Lists.newArrayList(),
                Lists.newArrayList(),
                context.getName(),
                context.getExternalLocation(),
                context.getS3AccessKey(),
                context.getS3SecretKey());
    }

    public DownloadSnapshotTask createDownloadSnapshotTask(
            CassandraDaemonTask daemon,
            RestoreContext context) {
        String name = DownloadSnapshotTask.nameForDaemon(daemon);
        String id = name + "_" + UUID.randomUUID().toString();

        return DownloadSnapshotTask.create(
                id,
                daemon.getSlaveId(),
                daemon.getHostname(),
                daemon.getExecutor(),
                name,
                daemon.getRole(),
                daemon.getPrincipal(),
                clusterTaskConfig.getCpus(),
                clusterTaskConfig.getMemoryMb(),
                clusterTaskConfig.getDiskMb(),
                DownloadSnapshotStatus.create(Protos.TaskState.TASK_STAGING,
                        id,
                        daemon.getSlaveId(),
                        name,
                        Optional.empty()),
                context.getName(),
                context.getExternalLocation(),
                context.getS3AccessKey(),
                context.getS3SecretKey(),
                cassandraConfig.getVolume().getPath() +
                        "/data/temp_" + context.getName());
    }

    public RestoreSnapshotTask createRestoreSnapshotTask(
            CassandraDaemonTask daemon,
            RestoreContext context) {
        String name = RestoreSnapshotTask.nameForDaemon(daemon);
        String id = name + "_" + UUID.randomUUID().toString();

        return RestoreSnapshotTask.create(
                id,
                daemon.getSlaveId(),
                daemon.getHostname(),
                daemon.getExecutor(),
                name,
                daemon.getRole(),
                daemon.getPrincipal(),
                clusterTaskConfig.getCpus(),
                clusterTaskConfig.getMemoryMb(),
                clusterTaskConfig.getDiskMb(),
                RestoreSnapshotStatus.create(Protos.TaskState.TASK_STAGING,
                        id,
                        daemon.getSlaveId(),
                        name,
                        Optional.empty()),
                context.getName(),
                context.getExternalLocation(),
                context.getS3AccessKey(),
                context.getS3SecretKey(),
                cassandraConfig.getVolume().getPath() +
                        "/data/temp_" + context.getName());
    }

    public BackupUploadTask createBackupUploadTask(
            CassandraDaemonTask daemon,
            BackupContext context) {
        String name = BackupUploadTask.nameForDaemon(daemon);
        String id = name + "_" + UUID.randomUUID().toString();

        return BackupUploadTask.create(
                id,
                daemon.getSlaveId(),
                daemon.getHostname(),
                daemon.getExecutor(),
                name,
                daemon.getRole(),
                daemon.getPrincipal(),
                clusterTaskConfig.getCpus(),
                clusterTaskConfig.getMemoryMb(),
                clusterTaskConfig.getDiskMb(),
                BackupUploadStatus.create(Protos.TaskState.TASK_STAGING,
                        id,
                        daemon.getSlaveId(),
                        name,
                        Optional.empty()),
                Lists.newArrayList(),
                Lists.newArrayList(),
                context.getName(),
                context.getExternalLocation(),
                context.getS3AccessKey(),
                context.getS3SecretKey(),
                cassandraConfig.getVolume().getPath() + "/data");
    }

    public CleanupTask createCleanupTask(
            CassandraDaemonTask daemon,
            CleanupContext context) {
        String name = CleanupTask.nameForDaemon(daemon);
        String id = name + "_" + UUID.randomUUID().toString();

        return CleanupTask.create(
                id,
                daemon.getSlaveId(),
                daemon.getHostname(),
                daemon.getExecutor(),
                name,
                daemon.getRole(),
                daemon.getPrincipal(),
                clusterTaskConfig.getCpus(),
                clusterTaskConfig.getMemoryMb(),
                clusterTaskConfig.getDiskMb(),
                CleanupStatus.create(Protos.TaskState.TASK_STAGING,
                        id,
                        daemon.getSlaveId(),
                        name,
                        Optional.empty()),
                context.getKeySpaces(),
                context.getColumnFamilies()
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
                executor.getDiskMb() == executorConfig.getDiskMb() &&
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

        return hasCurrentExecutorConfig(task.getExecutor()) &&
                task.getConfig().equals(cassandraConfig);

    }

    public CassandraDaemonTask updateConfig(final CassandraDaemonTask task) {

        String id = task.getName() + "_" + UUID.randomUUID().toString();
        return CassandraDaemonTask.create(
                id,
                task.getSlaveId(),
                task.getHostname(),
                updateExecutor(task, id),
                task.getName(),
                task.getRole(),
                task.getPrincipal(),
                cassandraConfig.getCpus(),
                cassandraConfig.getMemoryMb(),
                cassandraConfig.getDiskMb(),
                cassandraConfig.mutable()
                        .setVolume(task.getConfig().getVolume())
                        .setApplication(cassandraConfig.getApplication()
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

    public List<String> getErrors() {
        return ImmutableList.copyOf(errors);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
