package com.mesosphere.dcos.cassandra.common.tasks;


import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.config.ServiceConfig;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableTask;
import io.dropwizard.lifecycle.Managed;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.state.SchedulerState;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Cassandra State Store
 */
public class CassandraState extends SchedulerState implements Managed {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraState.class);

    private final ConfigurationManager configuration;
    private final ClusterTaskConfig clusterTaskConfig;

    // Maps Task Name -> Task, where task name can be PREFIX-id
    private volatile Map<String, CassandraTask> tasks = Collections.emptyMap();
    // Maps TaskId -> Task Name
    private final Map<String, String> byId = new HashMap<>();

    @Inject
    public CassandraState(
            final ConfigurationManager configuration,
            final ClusterTaskConfig clusterTaskConfig,
            final StateStore stateStore) {
        super(stateStore);
        this.configuration = configuration;
        this.clusterTaskConfig = clusterTaskConfig;

        loadTasks();
    }

    private void loadTasks() {
        Map<String, CassandraTask> builder = new HashMap<>();
        // Need to synchronize here to be sure that when the start method of
        // client managed objects is called this completes prior to the
        // retrieval of tasks
        try {
            synchronized (getStateStore()) {
                LOGGER.debug("Loading data from persistent store");
                final Collection<Protos.TaskInfo> taskInfos = getStateStore().fetchTasks();

                for (Protos.TaskInfo taskInfo : taskInfos) {
                    try {
                        final CassandraTask cassandraTask = CassandraTask.parse(TaskUtils.unpackTaskInfo(taskInfo));
                        LOGGER.debug("Loaded task: {}, type: {}, hostname: {}",
                                cassandraTask.getName(), cassandraTask.getType().name(), cassandraTask.getHostname());
                        builder.put(cassandraTask.getName(), cassandraTask);
                    } catch (IOException e) {
                        LOGGER.error("Error parsing task: {}. Reason: {}", TextFormat.shortDebugString(taskInfo), e);
                        throw new RuntimeException(e);
                    }
                }

                tasks = ImmutableMap.copyOf(builder);
                tasks.forEach((name, task) -> {
                    byId.put(task.getId(), name);
                });
                LOGGER.debug("Loaded tasks: {}", tasks);
            }
        } catch (StateStoreException e) {
            LOGGER.error("Error loading tasks. Reason: {}", e);
            throw new RuntimeException(e);
        }
    }


    private void removeTask(final String name) throws PersistenceException {
        getStateStore().clearTask(name);
        if (tasks.containsKey(name)) {
            byId.remove(tasks.get(name).getId());
        }
        tasks = ImmutableMap.<String, CassandraTask>builder().putAll(
                tasks.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(name))
                        .collect(Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> entry.getValue())))
                .build();
    }

    public Map<String, CassandraDaemonTask> getDaemons() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.CASSANDRA_DAEMON).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (CassandraDaemonTask) entry.getValue())));
    }

    public Map<String, BackupSnapshotTask> getBackupSnapshotTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.BACKUP_SNAPSHOT).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (BackupSnapshotTask) entry.getValue())));
    }

    public Map<String, BackupSchemaTask> getBackupSchemaTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.BACKUP_SCHEMA).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (BackupSchemaTask) entry.getValue())));
    }

    public Map<String, BackupUploadTask> getBackupUploadTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.BACKUP_UPLOAD).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (BackupUploadTask) entry.getValue())));
    }

    public Map<String, DownloadSnapshotTask> getDownloadSnapshotTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.SNAPSHOT_DOWNLOAD).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (DownloadSnapshotTask) entry.getValue())));
    }

    public Map<String, RestoreSnapshotTask> getRestoreSnapshotTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.SNAPSHOT_RESTORE).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (RestoreSnapshotTask) entry.getValue())));
    }

    public Map<String, RestoreSchemaTask> getRestoreSchemaTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.SCHEMA_RESTORE).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (RestoreSchemaTask) entry.getValue())));
    }

    public Map<String, CleanupTask> getCleanupTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.CLEANUP).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (CleanupTask) entry.getValue())));
    }

    public Map<String, RepairTask> getRepairTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.REPAIR).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (RepairTask) entry.getValue())));
    }

    public Map<String, UpgradeSSTableTask> getUpgradeSSTableTasks() {
        refreshTasks();
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.UPGRADESSTABLE).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (UpgradeSSTableTask) entry.getValue())));
    }

    public CassandraContainer createCassandraContainer(CassandraDaemonTask daemonTask) throws PersistenceException {
        CassandraTemplateTask templateTask = CassandraTemplateTask.create(
                daemonTask, clusterTaskConfig);
        return CassandraContainer.create(daemonTask, templateTask);
    }

    public CassandraContainer createCassandraContainer(CassandraDaemonTask daemonTask,
                                                       CassandraTemplateTask templateTask) throws PersistenceException {
        return CassandraContainer.create(daemonTask, templateTask);
    }

    public CassandraContainer createCassandraContainer(
            String name, String replaceIp) throws ConfigStoreException, PersistenceException {
        CassandraDaemonTask daemonTask = createReplacementDaemon(name, replaceIp);
        return createCassandraContainer(daemonTask, CassandraTemplateTask.create(daemonTask, clusterTaskConfig));
    }

    public CassandraContainer moveCassandraContainer(CassandraDaemonTask name)
            throws PersistenceException, ConfigStoreException {
        return createCassandraContainer(moveDaemon(name));
    }

    public CassandraContainer getOrCreateContainer(String name) throws PersistenceException, ConfigStoreException {
        final CassandraDaemonTask daemonTask = getOrCreateDaemon(name);
        return createCassandraContainer(daemonTask,
                getOrCreateTemplateTask(CassandraTemplateTask.toTemplateTaskName(name), daemonTask));
    }

    public CassandraTemplateTask getOrCreateTemplateTask(String name, CassandraDaemonTask daemonTask) {
        final Optional<CassandraTask> cassandraTask = get(name);
        if (cassandraTask.isPresent()) {
            return (CassandraTemplateTask) cassandraTask.get();
        } else {
            return CassandraTemplateTask.create(daemonTask, clusterTaskConfig);
        }
    }

    public CassandraDaemonTask createDaemon(String name) throws
            PersistenceException, ConfigStoreException {
        final CassandraSchedulerConfiguration targetConfig = configuration.getTargetConfig();
        final UUID targetConfigName = configuration.getTargetConfigName();
        final ServiceConfig serviceConfig = targetConfig.getServiceConfig();
        final String frameworkId = getStateStore().fetchFrameworkId().get().getValue();
        return configuration.createDaemon(
                frameworkId,
                name,
                serviceConfig.getRole(),
                serviceConfig.getPrincipal(),
                targetConfigName.toString()
        );
    }

    public CassandraDaemonTask createReplacementDaemon(String name, String replaceIp) throws
            PersistenceException, ConfigStoreException {
        final CassandraSchedulerConfiguration targetConfig = configuration.getTargetConfig();
        final UUID targetConfigName = configuration.getTargetConfigName();
        final ServiceConfig serviceConfig = targetConfig.getServiceConfig();
        final String frameworkId = getStateStore().fetchFrameworkId().get().getValue();

        final CassandraConfig.Builder configBuilder = new CassandraConfig.Builder(targetConfig.getCassandraConfig());
        configBuilder.setReplaceIp(replaceIp);

        return configuration.createDaemon(
                frameworkId,
                name,
                serviceConfig.getRole(),
                serviceConfig.getPrincipal(),
                targetConfigName.toString(),
                configBuilder.build());
    }

    public CassandraDaemonTask moveDaemon(CassandraDaemonTask daemon)
            throws PersistenceException, ConfigStoreException {
        final CassandraSchedulerConfiguration targetConfig = configuration.getTargetConfig();
        final ServiceConfig serviceConfig = targetConfig.getServiceConfig();
        CassandraDaemonTask updated = configuration.moveDaemon(
                daemon,
                getStateStore().fetchFrameworkId().get().getValue(),
                serviceConfig.getRole(),
                serviceConfig.getPrincipal());
        update(updated);

        Optional<Protos.TaskInfo> templateOptional = getTemplate(updated);
        if (templateOptional.isPresent()) {
            getStateStore().storeTasks(
                    Arrays.asList(CassandraTemplateTask.create(updated, clusterTaskConfig).getTaskInfo()));
        }

        return updated;
    }

    private Optional<Protos.TaskInfo> getTemplate(CassandraDaemonTask daemon) {
        String templateTaskName = CassandraTemplateTask.toTemplateTaskName(daemon.getName());
        try {
            Optional<Protos.TaskInfo> info = getStateStore().fetchTask(templateTaskName);
            LOGGER.info("Fetched template task for daemon '{}': {}",
                    daemon.getName(), TextFormat.shortDebugString(info.get()));
            return info;
        } catch (Exception e) {
            LOGGER.warn(String.format(
                    "Failed to retrieve template task '%s'", templateTaskName), e);
            return Optional.empty();
        }
    }


    public BackupSnapshotTask createBackupSnapshotTask(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return BackupSnapshotTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }

    }

    public BackupSchemaTask createBackupSchemaTask(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return BackupSchemaTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public BackupUploadTask createBackupUploadTask(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return BackupUploadTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public DownloadSnapshotTask createDownloadSnapshotTask(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return DownloadSnapshotTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public RestoreSnapshotTask createRestoreSnapshotTask(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return RestoreSnapshotTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public RestoreSchemaTask createRestoreSchemaTask(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return RestoreSchemaTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public CleanupTask createCleanupTask(
            CassandraDaemonTask daemon,
            CleanupContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return CleanupTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public RepairTask createRepairTask(
            CassandraDaemonTask daemon,
            RepairContext context) throws PersistenceException {
        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return RepairTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public UpgradeSSTableTask createUpgradeSSTableTask(
            CassandraDaemonTask daemon,
            UpgradeSSTableContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return UpgradeSSTableTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public CassandraDaemonTask getOrCreateDaemon(String name) throws
            PersistenceException, ConfigStoreException {
        if (getDaemons().containsKey(name)) {
            return getDaemons().get(name);
        } else {
            return createDaemon(name);
        }

    }

    public BackupSnapshotTask getOrCreateBackupSnapshot(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        String name = BackupSnapshotTask.nameForDaemon(daemon);
        Map<String, BackupSnapshotTask> snapshots = getBackupSnapshotTasks();
        if (snapshots.containsKey(name)) {
            return snapshots.get(name);
        } else {
            return createBackupSnapshotTask(daemon, context);
        }

    }

    public BackupSchemaTask getOrCreateBackupSchema(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        String name = BackupSchemaTask.nameForDaemon(daemon);
        Map<String, BackupSchemaTask> schemas = getBackupSchemaTasks();
        if (schemas.containsKey(name)) {
            return schemas.get(name);
        } else {
            return createBackupSchemaTask(daemon, context);
        }
    }

    public BackupUploadTask getOrCreateBackupUpload(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        String name = BackupUploadTask.nameForDaemon(daemon);
        Map<String, BackupUploadTask> uploads = getBackupUploadTasks();
        if (uploads.containsKey(name)) {
            return uploads.get(name);
        } else {
            return createBackupUploadTask(daemon, context);
        }

    }

    public DownloadSnapshotTask getOrCreateSnapshotDownload(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        String name = DownloadSnapshotTask.nameForDaemon(daemon);
        Map<String, DownloadSnapshotTask> snapshots = getDownloadSnapshotTasks();
        if (snapshots.containsKey(name)) {
            return snapshots.get(name);
        } else {
            return createDownloadSnapshotTask(daemon, context);
        }
    }

    public RestoreSnapshotTask getOrCreateRestoreSnapshot(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        String name = RestoreSnapshotTask.nameForDaemon(daemon);
        Map<String, RestoreSnapshotTask> snapshots = getRestoreSnapshotTasks();
        if (snapshots.containsKey(name)) {
            return snapshots.get(name);
        } else {
            return createRestoreSnapshotTask(daemon, context);
        }
    }

    public RestoreSchemaTask getOrCreateRestoreSchema(
            CassandraDaemonTask daemon,
            BackupRestoreContext context) throws PersistenceException {

        String name = RestoreSchemaTask.nameForDaemon(daemon);
        Map<String, RestoreSchemaTask> schemas = getRestoreSchemaTasks();
        if (schemas.containsKey(name)) {
            return schemas.get(name);
        } else {
            return createRestoreSchemaTask(daemon, context);
        }
    }

    public CleanupTask getOrCreateCleanup(
            CassandraDaemonTask daemon,
            CleanupContext context) throws PersistenceException {

        String name = CleanupTask.nameForDaemon(daemon);
        Map<String, CleanupTask> cleanups = getCleanupTasks();
        if (cleanups.containsKey(name)) {
            return cleanups.get(name);
        } else {
            return createCleanupTask(daemon, context);
        }
    }

    public RepairTask getOrCreateRepair(
            CassandraDaemonTask daemon,
            RepairContext context) throws PersistenceException {

        String name = RepairTask.nameForDaemon(daemon);
        Map<String, RepairTask> repairs = getRepairTasks();
        if (repairs.containsKey(name)) {
            return repairs.get(name);
        } else {
            return createRepairTask(daemon, context);
        }
    }

    public UpgradeSSTableTask getOrCreateUpgradeSSTable(
            CassandraDaemonTask daemon,
            UpgradeSSTableContext context) throws PersistenceException {

        String name = UpgradeSSTableTask.nameForDaemon(daemon);
        Map<String, UpgradeSSTableTask> upgradesstables = getUpgradeSSTableTasks();
        if (upgradesstables.containsKey(name)) {
            return upgradesstables.get(name);
        } else {
            return createUpgradeSSTableTask(daemon, context);
        }
    }

    public boolean needsConfigUpdate(final CassandraDaemonTask daemon) throws ConfigStoreException {
        return !configuration.hasCurrentConfig(daemon);
    }

    public CassandraDaemonTask replaceDaemon(CassandraDaemonTask task)
            throws PersistenceException {
        synchronized (getStateStore()) {
            return configuration.replaceDaemon(task);
        }
    }

    public CassandraDaemonTask reconfigureDaemon(
            final CassandraDaemonTask daemon) throws PersistenceException, ConfigStoreException {
        synchronized (getStateStore()) {
            return configuration.updateConfig(daemon);
        }
    }

    public void update(CassandraTask task) throws PersistenceException {
        synchronized (getStateStore()) {
            getStateStore().storeTasks(Arrays.asList(TaskUtils.packTaskInfo(task.getTaskInfo())));
            if (tasks.containsKey(task.getName())) {
                byId.remove(tasks.get(task.getName()).getId());
            }

            if (!task.getId().contains("__")) {
                LOGGER.error(
                        "Encountered malformed TaskID: " + task.getId(),
                        new PersistenceException("Encountered malformed TaskID: " + task.getId()));
            }

            byId.put(task.getId(), task.getName());
            tasks = ImmutableMap.<String, CassandraTask>builder().putAll(
                    tasks.entrySet().stream()
                            .filter(entry -> !entry.getKey().equals(task.getName()))
                            .collect(Collectors.toMap(
                                    entry -> entry.getKey(),
                                    entry -> entry.getValue())))
                    .put(task.getName(), task)
                    .build();
        }

        notifyObservers();
    }

    public void update(Protos.TaskInfo taskInfo, Offer offer) throws Exception {
        try {
            CassandraTask task = CassandraTask.parse(taskInfo);
            task = task.update(offer);
            getStateStore().storeTasks(Arrays.asList(TaskUtils.packTaskInfo(task.getTaskInfo())));
            update(task);
        } catch (Exception e) {
            LOGGER.error("Error storing task: {}, reason: {}", taskInfo, e);
            throw e;
        }
    }

    @Subscribe
    public void update(Protos.TaskStatus status) throws IOException {
        LOGGER.info("Received status update: {}", TextFormat.shortDebugString(status));
        synchronized (getStateStore()) {
            try {
                if (status.hasData()) {
                    getStateStore().storeStatus(status);
                } else {
                    Optional<Protos.TaskStatus> taskStatusOptional =
                            getStateStore().fetchStatus(TaskUtils.toTaskName(status.getTaskId()));

                    if (taskStatusOptional.isPresent()) {
                        if (taskStatusOptional.get().hasData()) {
                            getStateStore().storeStatus(Protos.TaskStatus.newBuilder(status)
                                    .setData(taskStatusOptional.get().getData())
                                    .build());
                        } else {
                            getStateStore().storeStatus(status);
                        }
                    } else {
                        getStateStore().storeStatus(status);
                    }
                }

                if (byId.containsKey(status.getTaskId().getValue())) {

                    CassandraTask cassandraTask = tasks.get(byId.get(status.getTaskId().getValue()));
                    if (cassandraTask.getState().equals(Protos.TaskState.TASK_FINISHED)
                            && status.getState().equals(Protos.TaskState.TASK_LOST)) {
                        LOGGER.warn("Ignoring TASK_LOST task update for finished Task.");
                        return;
                    }

                    if (status.hasData()) {
                        cassandraTask = cassandraTask.update(CassandraTaskStatus.parse(status));
                    } else {
                        cassandraTask = cassandraTask.update(status.getState());
                    }

                    update(cassandraTask);
                    LOGGER.info("Updated status for task {}", status.getTaskId().getValue());
                } else {
                    LOGGER.info("Received status update for unrecorded task: " +
                            "status = {}", status);
                    LOGGER.info("Tasks = {}", tasks);
                    LOGGER.info("Ids = {}", byId);
                }
            } catch (StateStoreException | TaskException e) {
                LOGGER.info("Unable to store status. Reason: ", e);
            }
        }
    }

    public boolean isTerminated(CassandraTask task) {
        try {
            final String name = task.getName();
            final Collection<String> taskNames = getStateStore().fetchTaskNames();
            if (CollectionUtils.isNotEmpty(taskNames) && taskNames.contains(name)) {
                final Optional<Protos.TaskStatus> status = getStateStore().fetchStatus(name);
                if (status.isPresent()) {
                    return CassandraDaemonStatus.isTerminated(status.get().getState());
                } else {
                    return false;
                }
            }
        } catch (StateStoreException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    public synchronized void refreshTasks() {
        LOGGER.info("Refreshing tasks");
        loadTasks();
    }

    public void remove(String name) throws PersistenceException {
        synchronized (getStateStore()) {
            if (tasks.containsKey(name)) {
                removeTask(name);
            }
        }
    }

    public void remove(Set<String> names) throws PersistenceException {
        for (String name : names) {
            remove(name);
        }
    }

    public Optional<CassandraTask> get(String name) {
        refreshTasks();
        return Optional.ofNullable(tasks.get(name));
    }

    public Map<String, CassandraTask> get() {
        return tasks;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    public Set<Protos.TaskStatus> getTaskStatuses() {
        return new HashSet<>(getStateStore().fetchStatuses());
    }
}
