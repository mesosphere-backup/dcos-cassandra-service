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
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * DownloadSnapshot implements the execution of the DownloadSnapshotTask by
 * delegating download of the snapshotted tables to a BackupStorageDriver
 * implementation.
 */
public class DownloadSnapshot implements ExecutorTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            DownloadSnapshot.class);
    private ExecutorDriver driver;
    private BackupRestoreContext context;
    private DownloadSnapshotTask cassandraTask;
    private BackupStorageDriver backupStorageDriver;

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {
        Protos.TaskStatus status = cassandraTask.createStatus(state,
            Optional.of(message)).getTaskStatus();
        driver.sendStatusUpdate(status);
    }

    /**
     * Constructs a DownloadSnapshot.
     *
     * @param driver              The ExecutorDriver used to send task status.
     * @param task                The DownloadSnapshotTask that will be executed.
     * @param backupStorageDriver The BackupStorageDriver that implements
     *                            downloading the snapshot.
     */
    public DownloadSnapshot(ExecutorDriver driver,
                            DownloadSnapshotTask task,
                            BackupStorageDriver backupStorageDriver) {
        this.driver = driver;
        this.backupStorageDriver = backupStorageDriver;
        this.cassandraTask = task;
        this.context = task.getBackupRestoreContext();
    }

    @Override
    public void run() {
        try {
            LOGGER.info("Starting DownloadSnapshot task using context: {}",
                    context);
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started downloading snapshot");

            // cleanup downloaded snapshot directory recursively if exists.
            Path rootPath = Paths.get(context.getLocalLocation() + File.separator + context.getName());
            if (rootPath.toFile().exists()) {
                Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
            backupStorageDriver.download(context);

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished downloading snapshots");
        } catch (Throwable t) {

            LOGGER.error("Download snapshot failed",t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        }
    }

    @Override
    public void stop(Future<?> future) {
        future.cancel(true);
    }
}
