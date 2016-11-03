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

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * Implements RestoreSnapshotTask by invoking the SSTableLoader binary that is
 * packaged with the Cassandra distribution.
 *
 * @TODO Why not just invoke the SSTableLoader class directly ins
 */
public class RestoreSnapshot implements ExecutorTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(
        RestoreSnapshot.class);

    private final ExecutorDriver driver;
    private final BackupRestoreContext context;
    private final RestoreSnapshotTask cassandraTask;
    private final CassandraDaemonProcess cassandra;

    /**
     * Constructs a new RestoreSnapshot.
     *
     * @param driver        The ExecutorDriver used to send task status.
     * @param cassandraTask The RestoreSnapshotTask that will be executed.
     * @param cassandra     The CassandraDaemonProcess running on the host
     */
    public RestoreSnapshot(
            ExecutorDriver driver,
            RestoreSnapshotTask cassandraTask,
            CassandraDaemonProcess cassandra) {
        this.driver = driver;
        this.cassandraTask = cassandraTask;
        this.context = cassandraTask.getBackupRestoreContext();
        this.cassandra = cassandra;
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started restoring snapshot");
            // run nodetool refresh rather than SSTableLoader, as on performance test
            // I/O stream was pretty slow between mesos container processes
            final String localLocation = context.getLocalLocation();
            final List<String> keyspaces = cassandra.getNonSystemKeySpaces();
            for (String keyspace : keyspaces) {
                final String keySpaceDirPath = localLocation + "/" + keyspace;
                File keySpaceDir = new File(keySpaceDirPath);
                File[] cfNames = keySpaceDir.listFiles(
                        (current, name) -> new File(current, name).isDirectory());
                for (File cfName : cfNames) {
                    String columnFamily = cfName.getName().substring(0, cfName.getName().indexOf("-"));
                    cassandra.getProbe().loadNewSSTables(keyspace, columnFamily);
                    LOGGER.info("Completed nodetool refresh for keyspace {} & columnfamily {}", keyspace, columnFamily);
                }
            }
            final String message = "Finished restoring snapshot";
            LOGGER.info(message);
            sendStatus(driver, Protos.TaskState.TASK_FINISHED, message);
        } catch (Throwable t) {
            // Send TASK_FAILED
            final String errorMessage = "Failed restoring snapshot. Reason: "
                    + t;
            LOGGER.error(errorMessage, t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        }
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {
        Protos.TaskStatus status = cassandraTask
            .createStatus(state, Optional.of(message)).getTaskStatus();
        driver.sendStatusUpdate(status);
    }

    @Override
    public void stop(Future<?> future) {
        future.cancel(true);
    }
}
