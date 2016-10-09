package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.backup.StorageDriverFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.apache.mesos.executor.ExecutorTaskException;
import org.apache.mesos.executor.ExecutorTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by gabriel on 9/20/16.
 */
public class CassandraTaskFactory implements ExecutorTaskFactory {
    private static final int DEFAULT_CORE_THREAD_POOL_SIZE = 10;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(DEFAULT_CORE_THREAD_POOL_SIZE);
    private final ExecutorDriver driver;
    private CassandraDaemonProcess cassandra;

    public CassandraTaskFactory(ExecutorDriver driver) {
       this.driver = driver;
    }

    public CassandraDaemonProcess getCassandra() {
        return cassandra;
    }

    @Override
    public ExecutorTask createTask(Protos.TaskInfo taskInfo, ExecutorDriver driver) throws ExecutorTaskException {

        CassandraTask cassandraTask = null;
        try {
            cassandraTask = CassandraTask.parse(taskInfo);
        } catch (IOException e) {
            throw new ExecutorTaskException(e);
        }

        switch(cassandraTask.getType()) {
            case CASSANDRA_DAEMON:
                try {
                    cassandra = CassandraDaemonProcess.create(scheduledExecutorService, taskInfo, driver);
                    return cassandra;
                } catch (IOException e) {
                    throw new ExecutorTaskException(e);
                }
            default:
                return launchClusterTask(cassandraTask);
        }
    }

    private ExecutorTask launchClusterTask(CassandraTask cassandraTask) throws ExecutorTaskException {
        if (cassandra == null || !cassandra.isOpen()) {
            throw new ExecutorTaskException(
                    "Failed to launch cluster task because the Cassandra Daemon is not running.");
        }

        switch (cassandraTask.getType()) {
            case BACKUP_SNAPSHOT:
                return new BackupSnapshot(driver, cassandra, (BackupSnapshotTask) cassandraTask);
            case BACKUP_UPLOAD:
                return new UploadSnapshot(
                    driver,
                    cassandra,
                    (BackupUploadTask) cassandraTask,
                    StorageDriverFactory.createStorageDriver(
                            (BackupUploadTask) cassandraTask));
            case SNAPSHOT_DOWNLOAD:
                return new DownloadSnapshot(
                        driver,
                        (DownloadSnapshotTask) cassandraTask,
                        StorageDriverFactory.createStorageDriver(
                                (DownloadSnapshotTask) cassandraTask));
            case SNAPSHOT_RESTORE:
                return new RestoreSnapshot(
                        driver,
                        (RestoreSnapshotTask) cassandraTask,
                        cassandra.getTask().getConfig().getVersion());
            case CLEANUP:
                return new Cleanup(driver, cassandra, (CleanupTask) cassandraTask);
            case REPAIR:
                return new Repair(driver, cassandra, (RepairTask) cassandraTask);
            default:
                Protos.TaskInfo info = cassandraTask.getTaskInfo();
                Protos.TaskStatus failed = Protos.TaskStatus
                        .newBuilder()
                        .setState(Protos.TaskState.TASK_FAILED)
                        .setSlaveId(info.getSlaveId())
                        .setExecutorId(info.getExecutor().getExecutorId())
                        .setTaskId(info.getTaskId())
                        .setMessage(String.format("Task not implemented: type = %s", cassandraTask.getType()))
                        .build();
                driver.sendStatusUpdate(failed);
                logger.error(String.format("Task not implemented: type = %s", cassandraTask.getType()));
                return null;
        }
    }
}
