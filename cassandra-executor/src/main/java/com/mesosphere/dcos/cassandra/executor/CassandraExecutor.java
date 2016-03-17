package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonStatus;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import com.mesosphere.dcos.cassandra.executor.backup.S3StorageDriver;
import com.mesosphere.dcos.cassandra.executor.tasks.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;


public class CassandraExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraExecutor.class);

    private volatile CassandraDaemonProcess cassandra;
    private String nodeId = null;
    private final ScheduledExecutorService executor;
    private final ExecutorService clusterJobExecutorService;

    private String getNodeId(String executorName) {
        int end = executorName.indexOf("_");
        return executorName.substring(0, end);
    }

    @Inject
    public CassandraExecutor(final ScheduledExecutorService executor,
                             final ExecutorService clusterJobExecutorService) {
        this.executor = executor;
        this.clusterJobExecutorService = clusterJobExecutorService;
    }


    @Override
    public void registered(ExecutorDriver driver,
                           Protos.ExecutorInfo executorInfo,
                           Protos.FrameworkInfo frameworkInfo,
                           Protos.SlaveInfo slaveInfo) {
        this.nodeId = getNodeId(executorInfo.getName());
    }

    @Override
    public void reregistered(ExecutorDriver driver,
                             Protos.SlaveInfo slaveInfo) {
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.warn("Executor disconnected");
    }

    @Override
    public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {
        try {
            LOGGER.info("Launching task: {}", task);

            CassandraTask cassandraTask = CassandraTask.parse(task);

            switch (cassandraTask.getType()) {
                case CASSANDRA_DAEMON:
                    if (cassandra != null && cassandra.isOpen()) {
                        Protos.TaskStatus daemonStatus = CassandraDaemonStatus
                                .create(
                                        Protos.TaskState.TASK_FAILED,
                                        cassandraTask.getId(),
                                        cassandraTask.getSlaveId(),
                                        cassandraTask.getExecutor().getId(),
                                        Optional.of(
                                                "Cassandra Daemon is already running"),
                                        CassandraMode.DRAINED).toProto();

                        driver.sendStatusUpdate(daemonStatus);

                        LOGGER.error("Cassandra Daemon is already running " +
                                        "status = {}",
                                daemonStatus);
                    } else {
                        cassandra = CassandraDaemonProcess.create(
                                (CassandraDaemonTask) cassandraTask,
                                executor,
                                driver
                        );

                        LOGGER.info("Starting Cassandra Daemon: task = {}",
                                cassandraTask);
                    }
                    break;

                case BACKUP_SNAPSHOT:
                    if (cassandra != null && cassandra.isOpen()) {
                        final NodeProbe probe = cassandra.getProbe();
                        final BackupSnapshot backupSnapshot = new BackupSnapshot(
                                driver, probe, cassandraTask);
                        clusterJobExecutorService.submit(backupSnapshot);
                    }
                    break;

                case BACKUP_UPLOAD:
                    if (cassandra != null && cassandra.isOpen()) {
                        final NodeProbe probe = cassandra.getProbe();
                        final BackupStorageDriver backupStorageDriver = new S3StorageDriver();
                        final UploadSnapshot uploadSnapshot = new UploadSnapshot(
                                driver,
                                probe,
                                cassandraTask,
                                nodeId,
                                backupStorageDriver);
                        clusterJobExecutorService.submit(uploadSnapshot);
                    }
                    break;

                case SNAPSHOT_DOWNLOAD:
                    if (cassandra != null && cassandra.isOpen()) {
                        final NodeProbe probe = cassandra.getProbe();
                        final BackupStorageDriver backupStorageDriver =
                                new S3StorageDriver();
                        final DownloadSnapshot downloadSnapshot =
                                new DownloadSnapshot(
                                        driver,
                                        probe,
                                        cassandraTask,
                                        nodeId,
                                        backupStorageDriver);
                        clusterJobExecutorService.submit(downloadSnapshot);
                    }
                    break;

                case SNAPSHOT_RESTORE:
                    if (cassandra != null && cassandra.isOpen()) {
                        final NodeProbe probe = cassandra.getProbe();
                        final RestoreSnapshot restoreSnapshot =
                                new RestoreSnapshot(
                                        driver,
                                        probe,
                                        cassandraTask,
                                        nodeId);
                        clusterJobExecutorService.submit(restoreSnapshot);
                    }
                    break;

                case CLEANUP:
                    if (cassandra != null && cassandra.isOpen()) {
                        final NodeProbe probe = cassandra.getProbe();
                        final Cleanup cleanup = new Cleanup(
                                driver, probe, (CleanupTask) cassandraTask);
                        clusterJobExecutorService.submit(cleanup);
                    }
                    break;

                default:
                    LOGGER.error("Unhandled task: type = {}",
                            cassandraTask.getType().name());
                    throw new NotImplementedException(
                            "Unsupported task type: {}",
                            cassandraTask.getType().name());
            }

        } catch (Throwable t) {
            LOGGER.error(String.format("Error launching task = %s", task), t);

            Protos.TaskStatus failed = Protos.TaskStatus
                    .newBuilder()
                    .setState(Protos.TaskState.TASK_FAILED)
                    .setSlaveId(task.getSlaveId())
                    .setExecutorId(task.getExecutor().getExecutorId())
                    .setTaskId(task.getTaskId())
                    .setMessage(String.format("Exception launching task %s",
                            t.getMessage())).build();

            driver.sendStatusUpdate(failed);

            LOGGER.error("Sent failed status update = {}", failed);
        }
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        if (cassandra != null && cassandra.isOpen()) {
            if (taskId.equals(cassandra.getTask().getId())) {
                LOGGER.info("Killing CassandraDaemon");
                cassandra.kill();
            } else {
                LOGGER.info("Unknown TaskId = ${}", taskId);
            }
        } else {
            LOGGER.error("CassandraDaemon is not running");
        }
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.error("You should not be using unreliable messaging buddy!");
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down now");
        if (cassandra != null && cassandra.isOpen()) {
            LOGGER.info("Disconnected - Killing Cassandra Daemon");
            cassandra.kill();
        }
        executor.shutdownNow();
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.error("Error {}", message);
    }

    public Optional<CassandraDaemonProcess> getCassandraDaemon() {
        return (cassandra != null && cassandra.isOpen()) ?
                Optional.of(cassandra) : Optional.empty();
    }
}
