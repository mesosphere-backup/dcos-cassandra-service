package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class DownloadSnapshot implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadSnapshot.class);
    private NodeProbe probe;
    private ExecutorDriver driver;
    private RestoreContext context;
    private DownloadSnapshotTask cassandraTask;
    private BackupStorageDriver backupStorageDriver;

    public DownloadSnapshot(ExecutorDriver driver, NodeProbe probe, CassandraTask cassandraTask, BackupStorageDriver backupStorageDriver) {
        this.probe = probe;
        this.driver = driver;
        this.backupStorageDriver = backupStorageDriver;
        this.cassandraTask = (DownloadSnapshotTask) cassandraTask;
        this.context = new RestoreContext();
        context.setName(this.cassandraTask.getBackupName());
        context.setExternalLocation(this.cassandraTask.getExternalLocation());
        context.setLocalLocation(this.cassandraTask.getLocalLocation());
        context.setS3AccessKey(this.cassandraTask.getS3AccessKey());
        context.setS3SecretKey(this.cassandraTask.getS3SecretKey());
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING, "Started downloading snapshot");

            backupStorageDriver.download(context);

            // TODO: Do cleanup (So, that we are good when we start restoring the snapshots)

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED, "Finished downloading snapshots");
        } catch (Exception e) {
            // Send TASK_FAILED
            final String errorMessage = "Failed downloading snapshot. Reason: " + e;
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
