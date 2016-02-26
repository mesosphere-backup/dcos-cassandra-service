package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RestoreSnapshot implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreSnapshot.class);
    private NodeProbe probe;
    private ExecutorDriver driver;
    private RestoreContext context;
    private RestoreSnapshotTask cassandraTask;
    private BackupStorageDriver backupStorageDriver;

    public RestoreSnapshot(ExecutorDriver driver, NodeProbe probe, CassandraTask cassandraTask, BackupStorageDriver backupStorageDriver) {
        this.probe = probe;
        this.driver = driver;
        this.backupStorageDriver = backupStorageDriver;
        this.cassandraTask = (RestoreSnapshotTask) cassandraTask;

        this.context = new RestoreContext();
        context.setName(this.cassandraTask.getBackupName());
        context.setS3AccessKey(this.cassandraTask.getS3AccessKey());
        context.setS3SecretKey(this.cassandraTask.getS3SecretKey());
        context.setExternalLocation(this.cassandraTask.getExternalLocation());
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING, "Started restoring snapshot");

            // TODO: Restore snapshot using sstableloader

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED, "Finished restoring snapshot");
        } catch (Exception e) {
            // Send TASK_FAILED
            final String errorMessage = "Failed restoring snapshot. Reason: " + e;
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
