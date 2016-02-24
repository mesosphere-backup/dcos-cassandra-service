package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class UploadSnapshot implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadSnapshot.class);
    private NodeProbe probe;
    private ExecutorDriver driver;
    final BackupContext backupContext;
    private BackupUploadTask cassandraTask;
    private BackupStorageDriver backupStorageDriver;

    public UploadSnapshot(ExecutorDriver driver, NodeProbe probe, CassandraTask cassandraTask, BackupStorageDriver backupStorageDriver) {
        this.probe = probe;
        this.driver = driver;
        this.cassandraTask = (BackupUploadTask) cassandraTask;
        this.backupStorageDriver = backupStorageDriver;
        backupContext = new BackupContext();
        backupContext.setName(this.cassandraTask.getBackupName());
        backupContext.setExternalLocation(this.cassandraTask.getExternalLocation());
        backupContext.setS3AccessKey(this.cassandraTask.getS3AccessKey());
        backupContext.setS3SecretKey(this.cassandraTask.getS3SecretKey());
        backupContext.setLocalLocation(this.cassandraTask.getLocalLocation());
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING, "Started uploading snapshots");

            backupStorageDriver.upload(backupContext);

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished uploading snapshots");
        } catch (Exception e) {
            // Send TASK_FAILED
            final String errorMessage = "Failed uploading snapshot. Reason: " + e;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        }
    }

    private void sendStatus(ExecutorDriver driver, Protos.TaskState state, String message) {
        Protos.TaskStatus status = BackupSnapshotStatus.create(
                state,
                cassandraTask.getId(),
                cassandraTask.getSlaveId(),
                cassandraTask.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }
}
