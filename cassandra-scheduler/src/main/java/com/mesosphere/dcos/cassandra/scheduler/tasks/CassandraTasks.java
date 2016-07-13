package com.mesosphere.dcos.cassandra.scheduler.tasks;


import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.CuratorFrameworkConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.reconciliation.TaskStatusProvider;
import org.apache.mesos.state.CuratorStateStore;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TaskStore for Cassandra framework tasks. It manages persisting and
 * retrieving
 */
public class CassandraTasks implements Managed, TaskStatusProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraTasks.class);

    private final IdentityManager identity;
    private final ConfigurationManager configuration;
    private final ClusterTaskConfig clusterTaskConfig;

    // Maps Task Name -> Task, where task name can be PREFIX-id
    private volatile Map<String, CassandraTask> tasks = Collections.emptyMap();
    // Maps TaskId -> Task Name
    private final Map<String, String> byId = new HashMap<>();
    private StateStore stateStore;

    @Inject
    public CassandraTasks(
            final IdentityManager identity,
            final ConfigurationManager configuration,
            final CuratorFrameworkConfig curatorConfig,
            final ClusterTaskConfig clusterTaskConfig) {
        this.identity = identity;
        this.configuration = configuration;
        this.clusterTaskConfig = clusterTaskConfig;

        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        this.stateStore = new CuratorStateStore(
                "/" + identity.get().getName() + "/state",
                curatorConfig.getServers(),
                retryPolicy);

        loadTasks();
    }

    private void loadTasks() {
        Map<String, CassandraTask> builder = new HashMap<>();
        // Need to synchronize here to be sure that when the start method of
        // client managed objects is called this completes prior to the
        // retrieval of tasks
        try {
            synchronized (stateStore) {
                LOGGER.info("Loading data from persistent store");
                final Collection<Protos.TaskInfo> taskInfos = stateStore.fetchTasks();

                for (Protos.TaskInfo taskInfo : taskInfos) {
                    try {
                        final CassandraTask cassandraTask = CassandraTask.parse(taskInfo);
                        LOGGER.info("Loaded task: {}", cassandraTask.getName());
                        builder.put(cassandraTask.getName(), cassandraTask);
                    } catch (IOException e) {
                        LOGGER.error("Error parsing task: {}. Reason: {}", taskInfo, e);
                        throw new RuntimeException(e);
                    }
                }

                tasks = ImmutableMap.copyOf(builder);
                tasks.forEach((name, task) -> {
                    byId.put(task.getId(), name);
                });
                LOGGER.info("Loaded tasks: {}", tasks);
            }
        } catch (StateStoreException e) {
            LOGGER.error("Error loading tasks. Reason: {}", e);
            throw new RuntimeException(e);
        }
    }


    private void removeTask(final String name) throws PersistenceException {
        stateStore.clearTask(name);
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

    public StateStore getStateStore() {
        return stateStore;
    }

    public Map<String, CassandraDaemonTask> getDaemons() {
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.CASSANDRA_DAEMON).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (CassandraDaemonTask) entry.getValue())));
    }

    public Map<String, BackupSnapshotTask> getBackupSnapshotTasks() {
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.BACKUP_SNAPSHOT).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (BackupSnapshotTask) entry.getValue())));
    }

    public Map<String, BackupUploadTask> getBackupUploadTasks() {
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.BACKUP_UPLOAD).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (BackupUploadTask) entry.getValue())));
    }

    public Map<String, DownloadSnapshotTask> getDownloadSnapshotTasks() {
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.SNAPSHOT_DOWNLOAD).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (DownloadSnapshotTask) entry.getValue())));
    }

    public Map<String, RestoreSnapshotTask> getRestoreSnapshotTasks() {
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.SNAPSHOT_RESTORE).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (RestoreSnapshotTask) entry.getValue())));
    }

    public Map<String, CleanupTask> getCleanupTasks() {
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.CLEANUP).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (CleanupTask) entry.getValue())));
    }

    public Map<String, RepairTask> getRepairTasks() {
        return tasks.entrySet().stream().filter(entry -> entry.getValue()
                .getType() == CassandraTask.TYPE.REPAIR).collect
                (Collectors.toMap(entry -> entry.getKey(), entry -> (
                        (RepairTask) entry.getValue())));
    }

    public CassandraContainer createCassandraContainer(CassandraDaemonTask daemonTask) throws PersistenceException {
        CassandraTemplateTask templateTask = CassandraTemplateTask.create(
                daemonTask, clusterTaskConfig);
        return CassandraContainer.create(daemonTask, templateTask);
    }

    public CassandraContainer moveCassandraContainer(CassandraDaemonTask name)
            throws PersistenceException, ConfigStoreException {
        return createCassandraContainer(moveDaemon(name));
    }

    public CassandraContainer getOrCreateContainer(String name) throws PersistenceException, ConfigStoreException {
        return createCassandraContainer(getOrCreateDaemon(name));
    }

    public CassandraDaemonTask createDaemon(String name) throws
            PersistenceException, ConfigStoreException {
        return configuration.createDaemon(
            identity.get().getId(),
            name,
            identity.get().getRole(),
            identity.get().getPrincipal()
        );
    }

    public CassandraDaemonTask moveDaemon(CassandraDaemonTask daemon)
            throws PersistenceException, ConfigStoreException {
        CassandraDaemonTask updated = configuration.moveDaemon(
            daemon,
            identity.get().getId(),
            identity.get().getRole(),
            identity.get().getPrincipal());
        update(updated);
        return updated;
    }

    private Optional<Protos.TaskInfo> getTemplate(CassandraDaemonTask daemon) {
            String templateTaskName = CassandraTemplateTask.toTemplateTaskName(daemon.getName());
        try {
            Optional<Protos.TaskInfo> info = Optional.of(stateStore.fetchTask(templateTaskName));
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
            BackupContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return BackupSnapshotTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }

    }

    public BackupUploadTask createBackupUploadTask(
            CassandraDaemonTask daemon,
            BackupContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return BackupUploadTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public DownloadSnapshotTask createDownloadSnapshotTask(
            CassandraDaemonTask daemon,
            RestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return DownloadSnapshotTask.create(template.get(), daemon, context);
        } else {
            throw new PersistenceException("Failed to retrieve ClusterTask Template.");
        }
    }

    public RestoreSnapshotTask createRestoreSnapshotTask(
            CassandraDaemonTask daemon,
            RestoreContext context) throws PersistenceException {

        Optional<Protos.TaskInfo> template = getTemplate(daemon);

        if (template.isPresent()) {
            return RestoreSnapshotTask.create(template.get(), daemon, context);
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
            BackupContext context) throws PersistenceException {

        String name = BackupSnapshotTask.nameForDaemon(daemon);
        Map<String, BackupSnapshotTask> snapshots = getBackupSnapshotTasks();
        if (snapshots.containsKey(name)) {
            return snapshots.get(name);
        } else {
            return createBackupSnapshotTask(daemon, context);
        }

    }

    public BackupUploadTask getOrCreateBackupUpload(
            CassandraDaemonTask daemon,
            BackupContext context) throws PersistenceException {

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
            RestoreContext context) throws PersistenceException {

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
            RestoreContext context) throws PersistenceException {

        String name = RestoreSnapshotTask.nameForDaemon(daemon);
        Map<String, RestoreSnapshotTask> snapshots = getRestoreSnapshotTasks();
        if (snapshots.containsKey(name)) {
            return snapshots.get(name);
        } else {
            return createRestoreSnapshotTask(daemon, context);
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

    public boolean needsConfigUpdate(final CassandraDaemonTask daemon) throws ConfigStoreException {
        return !configuration.hasCurrentConfig(daemon);
    }

    public CassandraDaemonTask replaceDaemon(CassandraDaemonTask task)
            throws PersistenceException {
        synchronized (stateStore) {
            return configuration.replaceDaemon(task);
        }
    }

    public CassandraDaemonTask reconfigureDeamon(
            final CassandraDaemonTask daemon) throws PersistenceException, ConfigStoreException {
        synchronized (stateStore) {
            return configuration.updateConfig(daemon);
        }
    }

    public void update(CassandraTask task) throws PersistenceException {
        synchronized (stateStore) {
            stateStore.storeTasks(Arrays.asList(task.getTaskInfo()));
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
    }

    public void update(Protos.TaskInfo taskInfo, Offer offer) throws Exception {
        try {
            final CassandraTask task = CassandraTask.parse(taskInfo);
            stateStore.storeTasks(Arrays.asList(taskInfo));

            synchronized (stateStore) {
                update(task.update(offer));
            }
        } catch (Exception e) {
            LOGGER.error("Error storing task: {}, reason: {}", taskInfo, e);
            throw e;
        }
    }

    @Subscribe
    public void update(Protos.TaskStatus status) throws IOException {
        synchronized (stateStore) {
            if (byId.containsKey(status.getTaskId().getValue())) {
                CassandraTask updated;

                if (status.hasData()) {
                    updated = tasks.get(
                            byId.get(status.getTaskId().getValue())).update(
                            CassandraTaskStatus.parse(status));
                } else {
                    updated = tasks.get(
                            byId.get(status.getTaskId().getValue())).update(
                            status.getState());
                }

                update(updated);

                stateStore.storeStatus(status);

                LOGGER.info("Updated task {}", updated);
            } else {
                LOGGER.info("Received status update for unrecorded task: " +
                        "status = {}", status);
                LOGGER.info("Tasks = {}", tasks);
                LOGGER.info("Ids = {}", byId);
            }
        }
    }

    public void remove(String name) throws PersistenceException {
        synchronized (stateStore) {
            if (tasks.containsKey(name)) {
                removeTask(name);
            }
        }
    }

    public Optional<CassandraTask> get(String name) {
        return Optional.ofNullable(tasks.get(name));
    }

    public Map<String, CassandraTask> get() {
        return tasks;
    }

    public List<CassandraTask> getTerminatedTasks() {
        List<CassandraTask> terminatedTasks = tasks
                .values().stream()
                .filter(task -> task.isTerminated()).collect(
                        Collectors.toList());

        return terminatedTasks;
    }

    public List<CassandraTask> getRunningTasks() {
        return tasks.values().stream()
                .filter(task -> isRunning(task)).collect(
                        Collectors.toList());

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    private boolean isRunning(CassandraTask task) {
        return Protos.TaskState.TASK_RUNNING == task.getState();
    }

    @Override
    public Set<Protos.TaskStatus> getTaskStatuses()  {
        return get().values().stream().map(
                task -> task.getCurrentStatus()).collect(Collectors.toSet());
    }

    public Protos.FrameworkID getFrameworkId() {
        try {
            return stateStore.fetchFrameworkId();
        } catch (StateStoreException e) {
            LOGGER.warn("Failed to get FrameworkID. "
                    + "This is expected when the service is starting for the first time.", e);
        }
        return null;
    }

    public void setFrameworkId(String frameworkId) throws StateStoreException {
        try {
            final Protos.FrameworkID fwkId = Protos.FrameworkID.newBuilder()
                    .setValue(frameworkId).build();
            LOGGER.info(String.format("Storing framework id: %s", fwkId));
            stateStore.storeFrameworkId(fwkId);
        } catch (StateStoreException e) {
            LOGGER.error("Failed to set FrameworkID: " + frameworkId, e);
            throw e;
        }
    }

    public boolean isRegistered() {
        final Protos.FrameworkID frameworkId = getFrameworkId();
        return frameworkId != null && frameworkId.hasValue();
    }
}
