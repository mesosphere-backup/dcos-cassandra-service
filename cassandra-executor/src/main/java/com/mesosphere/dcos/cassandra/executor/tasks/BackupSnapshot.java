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
package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Implements the execution of BackupSnapshot by executing the snapshot
 * method of the CassandraDaemonProcess and reporting status via the
 * ExecutorDriver.
 */
public class BackupSnapshot implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupSnapshot.class);
    private CassandraDaemonProcess daemon;
    private ExecutorDriver driver;
    private BackupSnapshotTask cassandraTask;

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {
        Protos.TaskStatus status = BackupSnapshotStatus.create(
                state,
                cassandraTask.getId(),
                cassandraTask.getSlaveId(),
                cassandraTask.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }

    /**
     * Constructs a BackupSnapshot.
     * @param driver The ExecutorDriver used to send task status.
     * @param daemon The CassandraDaemonProcess used to perform the snapshot.
     * @param cassandraTask The CassandraTask that will be executed by the
     *                      BackupSnapshot.
     */
    public BackupSnapshot(ExecutorDriver driver,
                          CassandraDaemonProcess daemon,
                          BackupSnapshotTask cassandraTask) {
        this.daemon = daemon;
        this.driver = driver;
        this.cassandraTask = cassandraTask;
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started taking snapshot");

            final String snapshotName = this.cassandraTask.getBackupName();
            final List<String> nonSystemKeyspaces = daemon
                    .getNonSystemKeySpaces();
            LOGGER.info("Started taking snapshot for non system keyspaces: {}",
                    nonSystemKeyspaces);
            for (String keyspace : nonSystemKeyspaces) {
                LOGGER.info("Taking snapshot for keyspace: {}", keyspace);
                daemon.takeSnapShot(snapshotName, keyspace);
            }

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished taking snapshot for non system keyspaces: " + nonSystemKeyspaces);
        } catch (Throwable t) {
            LOGGER.error("Snapshot failed",t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        }
    }
}
