/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonStatus;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.executor.backup.S3StorageDriver;
import com.mesosphere.dcos.cassandra.executor.tasks.*;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * CassandraExecutor implements the Executor for the framework. It is
 * responsible for launching the CassandraDaemonProcess and any ClusterTasks
 * that are executed for the Cassandra node.
 * Note that, unless the CassandraDaemonProcess is running, Cluster tasks
 * will not be able to execute.
 */
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

    private void launchDeamon(
            final CassandraTask task,
            final ExecutorDriver driver) throws IOException {
        if (cassandra != null && cassandra.isOpen()) {
            Protos.TaskStatus daemonStatus = CassandraDaemonStatus
                    .create(
                            Protos.TaskState.TASK_FAILED,
                            task.getId(),
                            task.getSlaveId(),
                            task.getExecutor().getId(),
                            Optional.of(
                                    "Cassandra Daemon is already running"),
                            CassandraMode.DRAINED).toProto();

            driver.sendStatusUpdate(daemonStatus);

            LOGGER.error("Cassandra Daemon is already running " +
                            "status = {}",
                    daemonStatus);
        } else {
            cassandra = CassandraDaemonProcess.create(
                    (CassandraDaemonTask) task,
                    executor,
                    driver
            );

            LOGGER.info("Starting Cassandra Daemon: task = {}",
                    task);
        }
    }

    private void launchClusterTask(
            final CassandraTask cassandraTask,
            final ExecutorDriver driver) {
        if (cassandra == null || !cassandra.isOpen()) {

            Protos.TaskInfo info = cassandraTask.toProto();
            Protos.TaskStatus failed = Protos.TaskStatus
                    .newBuilder()
                    .setState(Protos.TaskState.TASK_FAILED)
                    .setSlaveId(info.getSlaveId())
                    .setExecutorId(info.getExecutor().getExecutorId())
                    .setTaskId(info.getTaskId())
                    .setMessage("Cannot launch cluster task as Cassandra " +
                            "Daemon is not running")
                    .build();
            driver.sendStatusUpdate(failed);
            LOGGER.error("Failed to launch cluster task because the Cassandra" +
                    " Daemon is not running: task = {}", failed);

            return;
        }

        switch (cassandraTask.getType()) {
            case BACKUP_SNAPSHOT:
                clusterJobExecutorService.submit(new BackupSnapshot(
                        driver,
                        cassandra,
                        (BackupSnapshotTask) cassandraTask));
                break;

            case BACKUP_UPLOAD:
                clusterJobExecutorService.submit(new UploadSnapshot(
                        driver,
                        cassandra,
                        (BackupUploadTask) cassandraTask,
                        nodeId,
                        new S3StorageDriver()));

                break;

            case SNAPSHOT_DOWNLOAD:
                clusterJobExecutorService.submit(new DownloadSnapshot(
                        driver,
                        (DownloadSnapshotTask) cassandraTask,
                        nodeId,
                        new S3StorageDriver()));

                break;

            case SNAPSHOT_RESTORE:

                clusterJobExecutorService.submit(new RestoreSnapshot(
                        driver,
                        (RestoreSnapshotTask) cassandraTask,
                        nodeId,
                        cassandra.getTask().getConfig().getVersion()));

                break;

            case CLEANUP:
                clusterJobExecutorService.submit(
                        new Cleanup(
                                driver,
                                cassandra,
                                (CleanupTask) cassandraTask));

                break;

            case REPAIR:
                clusterJobExecutorService.submit(new Repair(
                        driver,
                        cassandra,
                        (RepairTask) cassandraTask));

                break;

            default:
                Protos.TaskInfo info = cassandraTask.toProto();
                Protos.TaskStatus failed = Protos.TaskStatus
                        .newBuilder()
                        .setState(Protos.TaskState.TASK_FAILED)
                        .setSlaveId(info.getSlaveId())
                        .setExecutorId(info.getExecutor().getExecutorId())
                        .setTaskId(info.getTaskId())
                        .setMessage(String.format("Task not implemented: type" +
                                " = %s", cassandraTask.getType()))
                        .build();
                driver.sendStatusUpdate(failed);
                LOGGER.error(String.format("Task not implemented: type" +
                        " = %s", cassandraTask.getType()));
        }
    }

    /**
     * Constructs a new CassandraExecutor
     * @param executor The ScheduledExecutorService used by the
     *                 CassandraDaemonProcess for its watchdog and monitoring
     *                 tasks.
     * @param clusterJobExecutorService The ExecutorService used by the
     *                                  Executor to run ClusterTasks.
     */
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
                    launchDeamon(cassandraTask, driver);
                    break;

                default:
                    launchClusterTask(cassandraTask, driver);
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
