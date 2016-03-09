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
import java.util.stream.Collectors;

/**
 * TaskStore for Cassandra framework tasks.
 */
public class CassandraTasks implements Managed {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTasks.class);
    private static final Set<Protos.TaskState> terminalStates = new HashSet<>(
            Arrays.asList(
                    Protos.TaskState.TASK_ERROR,
                    Protos.TaskState.TASK_FAILED,
                    Protos.TaskState.TASK_FINISHED,
                    Protos.TaskState.TASK_KILLED,
                    Protos.TaskState.TASK_LOST));

    private final IdentityManager identity;
    private final ConfigurationManager configuration;
    private final PersistentMap<CassandraTask> persistent;

    // Maps Task Name -> Task, where task name can be PREFIX-id
    private volatile Map<String, CassandraTask> tasks = Collections.emptyMap();
    // Maps TaskId -> Task Name
    private final Map<String, String> byId = new HashMap<>();

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
                tasks.forEach((name, task) -> {
                    byId.put(task.getId(), name);
                });
                LOGGER.info("Loaded tasks: {}", tasks);
                long max = getDaemons().values().stream().map(task ->
                        Integer.parseInt(
                                task.getName().replace(
                                        CassandraDaemonTask.NAME_PREFIX, "")
                        )).max(Integer::compare).orElse(0);

            }
        } catch (PersistenceException e) {
            LOGGER.error("Error loading tasks. Reason: {}", e);
            throw new RuntimeException(e);
        }
    }

    public void update(CassandraTask task) throws PersistenceException {
        persistent.put(task.getName(), task);
        if (tasks.containsKey(task.getName())) {
            byId.remove(tasks.get(task.getName()).getId());
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


    public void update(Protos.TaskInfo taskInfo) {
        try {
            final CassandraTask task = CassandraTask.parse(taskInfo);
            update(task);
        } catch (Exception e) {
            LOGGER.error("Error storing task: {}, reason: {}", taskInfo, e);
        }
    }

    private void removeTask(String name) throws PersistenceException {
        persistent.remove(name);
        if (tasks.containsKey(name)) {
            byId.remove(tasks.get(name).getId());
        }
        tasks = ImmutableMap.<String, CassandraTask>builder().putAll(
                tasks.entrySet().stream()
                        .filter(entry -> entry.getKey() != name)
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

    public CassandraDaemonTask createDaemon(String name) throws
            PersistenceException {
        CassandraDaemonTask task = configuration.createDaemon(
                identity.get().getId().get(),
                "",
                "",
                name,
                identity.get().getRole(),
                identity.get().getPrincipal()
        );

        synchronized (persistent) {
            update(task);
        }

        return task;
    }

    private Optional<CassandraTaskExecutor> getExecutorFromNodeId(int num) {
        // Get the executorInfo from store
        final Optional<String> taskId = tasks.keySet().stream().filter(entry -> TaskUtils.taskNameToNodeId(entry) == num).findFirst();
        if (!taskId.isPresent()) {
            return Optional.empty();
        }

        final CassandraTask cassandraTask = tasks.get(taskId.get());
        return Optional.ofNullable(cassandraTask.getExecutor());
    }

    public BackupSnapshotTask createBackupSnapshotTask(int num, BackupContext backupContext) throws PersistenceException {
        final Optional<CassandraTaskExecutor> executor = getExecutorFromNodeId(num);

        if (!executor.isPresent()) {
            LOGGER.error("Cannot find executor associated with node id: {}", num);
            return null;
        }

        final Identity identity = this.identity.get();
        final BackupSnapshotTask task = configuration.createBackupSnapshotTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor.get(),
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
        final Optional<CassandraTaskExecutor> executor = getExecutorFromNodeId(num);

        if (!executor.isPresent()) {
            LOGGER.error("Cannot find executor associated with node id: {}", num);
            return null;
        }

        final Identity identity = this.identity.get();
        final BackupUploadTask task = configuration.createBackupUploadTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor.get(),
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
        final Optional<CassandraTaskExecutor> executor = getExecutorFromNodeId(num);

        if (!executor.isPresent()) {
            LOGGER.error("Cannot find executor associated with node id: {}", num);
            return null;
        }
        final Identity identity = this.identity.get();
        final DownloadSnapshotTask task = configuration.createDownloadSnapshotTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor.get(),
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
        final Optional<CassandraTaskExecutor> executor = getExecutorFromNodeId(num);

        if (!executor.isPresent()) {
            LOGGER.error("Cannot find executor associated with node id: {}", num);
            return null;
        }
        final Identity identity = this.identity.get();
        final RestoreSnapshotTask task = configuration.createRestoreSnapshotTask(
                identity.getId().get(),
                "", /* SlaveId */
                executor.get(),
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

    public CassandraDaemonTask getOrCreateDaemon(String name) throws
            PersistenceException {
        if(getDaemons().containsKey(name)){
            return getDaemons().get(name);
        } else{
            return createDaemon(name);
        }

    }

    public boolean needsConfigUpdate(final CassandraDaemonTask daemon) {
        return !configuration.hasCurrentConfig(daemon);
    }

    public CassandraTask replaceTask(CassandraTask task)
            throws PersistenceException {

        synchronized (persistent){
            final CassandraDaemonTask updated =
                    configuration.replaceDaemon((CassandraDaemonTask)task);
            update(updated);
            return updated;
        }

    }

    public CassandraDaemonTask replaceDaemon(CassandraDaemonTask task)
            throws PersistenceException{
        synchronized (persistent){
            final CassandraDaemonTask updated =
                    configuration.replaceDaemon(task);
            update(updated);
            return updated;
        }
    }

    public CassandraDaemonTask reconfigureDeamon(
            final CassandraDaemonTask daemon) throws PersistenceException {
        synchronized (persistent) {
            final CassandraDaemonTask updated = configuration.updateConfig(
                    daemon);
            update(updated);
            return updated;
        }
    }

    public Optional<CassandraTask> update(String taskId, Protos.Offer offer)
            throws PersistenceException {
        synchronized (persistent) {
            if (byId.containsKey(taskId)) {
                CassandraTask updated = tasks.get(byId.get(taskId)).update(
                        offer);
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
                LOGGER.info("Updated task {}", updated);
            } else {
                LOGGER.info("Received status update for unrecorded task: " +
                        "status = {}", status);
            }
        }
    }

    public void remove(String name) throws PersistenceException {
        synchronized (persistent) {
            if (tasks.containsKey(name)) removeTask(name);
        }
    }

    public void removeById(String id) throws PersistenceException {
        synchronized (persistent) {
            if (byId.containsKey(id)) {
                removeTask(byId.get(id));
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

    public Optional<CassandraTask> findCassandraDaemonTaskbyId(int id) {
        final String prefix = CassandraDaemonTask.NAME_PREFIX + id;
        CassandraTask task = null;
        for (String taskId : tasks.keySet()) {
            if (taskId.startsWith(prefix)) {
                task = tasks.get(taskId);
                break;
            }
        }

        return Optional.of(task);
    }
}
