package com.mesosphere.dcos.cassandra.scheduler.tasks;


import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentMap;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * TaskStore for Cassandra framework tasks.
 */
public class CassandraTasks implements Managed {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTasks.class);

    private final IdentityManager identity;
    private final ConfigurationManager configuration;
    private final AtomicLong serverId = new AtomicLong(0);
    private final PersistentMap<CassandraTask> persistent;
    private volatile Map<String, CassandraTask> tasks = Collections.emptyMap();

    @Inject
    public CassandraTasks(
            final IdentityManager identity,
            final ConfigurationManager configuration,
            final Serializer<CassandraTask> serializer,
            final PersistenceFactory persistence) {
        this.persistent = persistence.createMap("tasks", serializer);
        this.identity = identity;
        this.configuration = configuration;
        loadTasks();
    }

    private void loadTasks() {
        Map<String, CassandraTask> builder = new HashMap<>();
        // Need to synchronize here to be sure that when the start method of
        // client managed objects is called this completes prior to the
        // retrieval of tasks
        try {
            synchronized (persistent) {
                LOGGER.info("Loading data from persistent store");
                for (String key : persistent.keySet()) {
                    LOGGER.info("Loaded key: {}", key);
                    builder.put(key, persistent.get(key).get());
                }
                tasks = ImmutableMap.copyOf(builder);
                LOGGER.info("Loaded tasks: {}", tasks);
                long max = getDaemons().values().stream().map(task -> Integer
                        .parseInt(
                                task.getName().replace(
                                        CassandraDaemonTask.NAME_PREFIX, "")
                        )).max(Integer::compare).orElse(0);

                serverId.set((tasks.isEmpty()) ? max : max + 1);
            }
        } catch (PersistenceException e) {
            LOGGER.error("Error loading tasks. Reason: {}", e);
            throw new RuntimeException(e);
        }
    }

    public void update(CassandraTask task) throws PersistenceException {
        persistent.put(task.getId(), task);
        tasks = ImmutableMap.<String, CassandraTask>builder().putAll(
                tasks.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(task.getId()))
                        .collect(Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> entry.getValue())))
                .put(task.getId(), task)
                .build();
    }

    public CassandraTask getCassandraTaskByNodeId(int nodeId) {
        for (CassandraTask task : tasks.values()) {
            final String taskId = task.getId();
            final int id = TaskUtils.taskIdToNodeId(taskId);
            LOGGER.info("For taskId: {} id is: {}", taskId, id);
            if (nodeId == id) {
                LOGGER.info("Found task: {} with nodeId: {}", task, nodeId);
                return task;
            }
        }

        LOGGER.info("Didn't find any task with nodeId: {}", nodeId);
        return null;
    }

    public void update(Protos.TaskInfo taskInfo) {
        try {
            final CassandraTask task = CassandraTask.parse(taskInfo);
            update(task);
        } catch (Exception e) {
            LOGGER.error("Error storing task: {}, reason: {}", taskInfo, e);
        }
    }

    private void removeTask(String taskId) throws PersistenceException {
        persistent.remove(taskId);
        tasks = ImmutableMap.<String, CassandraTask>builder().putAll(
                tasks.entrySet().stream()
                        .filter(entry -> entry.getKey() != taskId)
                        .collect(Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> entry.getValue())))
                .build();
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

    public CassandraDaemonTask createDaemon() throws PersistenceException {
        CassandraDaemonTask task = configuration.createDaemon(
                identity.get().getId().get(),
                "",
                "",
                CassandraDaemonTask.NAME_PREFIX + serverId.getAndIncrement(),
                identity.get().getRole(),
                identity.get().getPrincipal()
        );

        synchronized (persistent) {
            update(task);
        }

        return task;
    }

    private CassandraTaskExecutor getExecutorFromNodeId(int num) {
        // Get the executorInfo from store
        final Optional<String> taskId = tasks.keySet().stream().filter(entry -> TaskUtils.taskIdToNodeId(entry) == num).findFirst();
        if (!taskId.isPresent()) {
            return null;
        }

        final CassandraTask cassandraTask = tasks.get(taskId.get());
        return cassandraTask.getExecutor();
    }

    public BackupSnapshotTask createBackupSnapshotTask(int num, BackupContext backupContext) throws PersistenceException {
        final CassandraTaskExecutor executor = getExecutorFromNodeId(num);
        final Identity identity = this.identity.get();
        final BackupSnapshotTask task = configuration.createBackupSnapshotTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor,
                "", /* Hostname */
                BackupSnapshotTask.NAME_PREFIX + num,
                identity.getRole(),
                identity.getPrincipal(),
                backupContext
        );

        synchronized (persistent) {
            update(task);
        }

        return task;
    }

    public BackupUploadTask createBackupUploadTask(int num, BackupContext backupContext) throws PersistenceException {
        final CassandraTaskExecutor executor = getExecutorFromNodeId(num);
        final Identity identity = this.identity.get();
        final BackupUploadTask task = configuration.createBackupUploadTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor,
                "", /* Hostname */
                BackupUploadTask.NAME_PREFIX + num,
                identity.getRole(),
                identity.getPrincipal(),
                backupContext
        );

        synchronized (persistent) {
            update(task);
        }

        return task;
    }

    public DownloadSnapshotTask createDownloadSnapshotTask(int num, RestoreContext context) throws PersistenceException {
        final CassandraTaskExecutor executor = getExecutorFromNodeId(num);
        final Identity identity = this.identity.get();
        final DownloadSnapshotTask task = configuration.createDownloadSnapshotTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor,
                "", /* Hostname */
                DownloadSnapshotTask.NAME_PREFIX + num,
                identity.getRole(),
                identity.getPrincipal(),
                context
        );

        synchronized (persistent) {
            update(task);
        }

        return task;
    }

    public RestoreSnapshotTask createRestoreSnapshotTask(int num, RestoreContext context) throws PersistenceException {
        final CassandraTaskExecutor executor = getExecutorFromNodeId(num);
        final Identity identity = this.identity.get();
        final RestoreSnapshotTask task = configuration.createRestoreSnapshotTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor,
                "", /* Hostname */
                RestoreSnapshotTask.NAME_PREFIX + num,
                identity.getRole(),
                identity.getPrincipal(),
                context
        );

        synchronized (persistent) {
            update(task);
        }

        return task;
    }

    public CassandraDaemonTask createDaemon(Protos.Offer offer) throws
            PersistenceException {
        CassandraDaemonTask task = configuration.createDaemon(
                identity.get().getId().get(),
                offer.getSlaveId().getValue(),
                offer.getHostname(),
                CassandraDaemonTask.NAME_PREFIX + serverId.getAndIncrement(),
                identity.get().getRole(),
                identity.get().getPrincipal()
        );

        synchronized (persistent) {
            update(task);
        }

        return task;
    }

    public boolean needsConfigUpdate(final CassandraDaemonTask daemon) {
        return !configuration.hasCurrentConfig(daemon);
    }

    public CassandraDaemonTask updateConfig(
            final CassandraDaemonTask daemon) throws PersistenceException {
        CassandraDaemonTask updated = configuration.updateConfig(daemon);
        update(updated);
        return updated;
    }

    public Optional<CassandraTask> update(String taskId, Protos.Offer offer)
            throws PersistenceException {
        synchronized (persistent) {
            if (tasks.containsKey(taskId)) {
                CassandraTask updated = tasks.get(taskId).update(offer);
                update(updated);
                return Optional.of(updated);
            } else {
                return Optional.empty();
            }
        }
    }

    @Subscribe
    public void update(Protos.TaskStatus status) throws IOException {
        synchronized (persistent) {
            if (tasks.containsKey(status.getTaskId().getValue())) {
                CassandraTask updated;

                if (status.hasData()) {
                    updated = tasks.get(
                            status.getTaskId().getValue()).update(
                            CassandraTaskStatus.parse(status));
                } else {
                    updated = tasks.get(
                            status.getTaskId().getValue()).update(
                            status.getState());
                }

                update(updated);
                LOGGER.info("Updated task {}", updated);
            } else {
                LOGGER.info("Received status update for unrecorded task: " +
                        "status = {}", status);
            }
        }
    }


    public void remove(String id) throws PersistenceException {
        synchronized (persistent) {
            if (tasks.containsKey(id)) removeTask(id);
        }
    }

    public Optional<CassandraTask> get(String id) {
        return Optional.ofNullable(tasks.get(id));
    }

    public Map<String, CassandraTask> get() {
        return tasks;
    }

    public List<CassandraTask> getTerminatedTasks() {
        List<CassandraTask> terminatedTasks = tasks
                .values().stream()
                .filter(task -> TaskUtils.isTerminated(
                        task.getStatus().getState())).collect(
                        Collectors.toList());

        return terminatedTasks;
    }

    public List<CassandraTask> getTasksToRepair() {
        List<CassandraTask> terminatedTasks = tasks
                .values().stream()
                .filter(task -> TaskUtils.needsRecheduling(task))
                .collect(Collectors.toList());

        return terminatedTasks;
    }


    public List<CassandraTask> getRunningTasks() {
        final List<CassandraTask> runningTasks = tasks.values().stream()
                .filter(task -> isRunning(task)).collect(Collectors.toList());
        return runningTasks;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    private boolean isRunning(CassandraTask task) {
        return Protos.TaskState.TASK_RUNNING == task.getStatus().getState();
    }
}
