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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import com.mesosphere.dcos.cassandra.executor.backup.StorageUtil;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * Implements the backing up of schema for current CassandraDaemonProcess
 * using Datastax Java Driver.
 */
public class BackupSchema implements ExecutorTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupSchema.class);
    private CassandraDaemonProcess daemon;
    private ExecutorDriver driver;
    private final BackupRestoreContext context;
    private BackupSchemaTask cassandraTask;
    private final BackupStorageDriver backupStorageDriver;
    private final StorageUtil storageUtil = new StorageUtil();

  private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {
        final Protos.TaskStatus status =
        cassandraTask.createStatus(state,Optional.of(message)).getTaskStatus();
        driver.sendStatusUpdate(status);
  }

    /**
     * Constructs a BackupSchema.
     * @param driver The ExecutorDriver used to send task status.
     * @param daemon The CassandraDaemonProcess used to fetch schema.
     * @param cassandraTask The CassandraTask that will be executed by the
     *                      BackupSchema.
     */
  public BackupSchema(ExecutorDriver driver,
                      CassandraDaemonProcess daemon,
                      BackupSchemaTask cassandraTask,
                      BackupStorageDriver backupStorageDriver) {
      this.daemon = daemon;
      this.driver = driver;
      this.cassandraTask = cassandraTask;
      this.backupStorageDriver = backupStorageDriver;
      context = cassandraTask.getBackupRestoreContext();
  }

  @Override
  public void run() {
      Cluster cluster = null;

      try {
          // Send TASK_RUNNING
          sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                  "Started taking schema backup");

          cluster = Cluster.builder().addContactPoint(daemon.getProbe().getEndpoint()).withCredentials(context.getUsername(), context.getPassword()).build();
          final List<String> keyspaces = StorageUtil.filterSystemKeyspaces(daemon.getNonSystemKeySpaces());

          if (keyspaces.size() > 0) {
              StringBuilder sb = new StringBuilder();
              for (String keyspace : keyspaces) {
                  LOGGER.info("Taking schema backup for keyspace: {}", keyspace);
                  KeyspaceMetadata ksm = cluster.getMetadata().getKeyspace(keyspace);
                  sb.append(ksm.exportAsString()).append(System.getProperty("line.separator"));
              }
              backupStorageDriver.uploadSchema(context, sb.toString());
          }

          // Send TASK_FINISHED
          sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                  "Finished taking schema backup for keyspaces: " + keyspaces);
      } catch (Throwable t){
          LOGGER.error("Schema backup failed. Reason: ", t);
          sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
      } finally {
          if (cluster != null)
              cluster.close();
      }
  }

  @Override
  public void stop(Future<?> future) {
      future.cancel(true);
  }
}
