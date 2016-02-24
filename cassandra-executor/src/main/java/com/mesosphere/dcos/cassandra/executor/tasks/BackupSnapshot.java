package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class BackupSnapshot implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupSnapshot.class);
    private NodeProbe probe;
    private ExecutorDriver driver;
    private BackupSnapshotTask cassandraTask;

    public BackupSnapshot(ExecutorDriver driver, NodeProbe probe, CassandraTask cassandraTask) {
        this.probe = probe;
        this.driver = driver;
        this.cassandraTask = (BackupSnapshotTask) cassandraTask;
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING, "Started taking snapshot");

            final String snapshotName = this.cassandraTask.getBackupName();
            final List<String> nonSystemKeyspaces = probe.getNonSystemKeyspaces();
            LOGGER.info("Started taking snapshot for non system keyspaces: {}",
                    nonSystemKeyspaces);
            for (String keyspace : nonSystemKeyspaces) {
                LOGGER.info("Taking snapshot for keyspace: {}", keyspace);
                probe.takeSnapshot(snapshotName, null, keyspace);
            }

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished taking snapshot for non system keyspaces: " + nonSystemKeyspaces);
        } catch (Exception e) {
            // Send TASK_FAILED
            final String errorMessage = "Failed taking snapshot. Reason: " + e;
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
