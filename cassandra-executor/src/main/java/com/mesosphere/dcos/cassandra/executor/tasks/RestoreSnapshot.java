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
import com.mesosphere.dcos.cassandra.executor.CassandraPaths;
import com.mesosphere.dcos.cassandra.executor.backup.StorageUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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
    private final String version;

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
        this.version = cassandra.getTask().getConfig().getVersion();

    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started restoring snapshot");

            if (Objects.equals(context.getRestoreType(), new String("new"))) {
                final String keyspaceDirectory =
                        context.getLocalLocation() + File.separator +
                                context.getName() + File.separator +
                                context.getNodeId();

                final String ssTableLoaderBinary =
                        CassandraPaths.create(version).bin()
                                .resolve("sstableloader").toString();
                final String cassandraYaml =
                        CassandraPaths.create(version).cassandraConfig().toString();

                final File keyspacesDirectory = new File(keyspaceDirectory);
                LOGGER.info("Keyspace Directory {} exists: {}", keyspaceDirectory, keyspacesDirectory.exists());

                final File[] keyspaces = keyspacesDirectory.listFiles();

                String libProcessAddress = System.getenv("LIBPROCESS_IP");
                libProcessAddress = StringUtils.isBlank(
                        libProcessAddress) ? InetAddress.getLocalHost().getHostAddress() : libProcessAddress;

                for (File keyspace : keyspaces) {
                    final File[] columnFamilies = keyspace.listFiles();

                    final String keyspaceName = keyspace.getName();
                    if (keyspaceName.equals(StorageUtil.SCHEMA_FILE))
                        continue;
                    LOGGER.info("Going to bulk load keyspace: {}", keyspaceName);


                    for (File columnFamily : columnFamilies) {
                        final String columnFamilyName = columnFamily.getName();
                        if (columnFamilyName.equals(StorageUtil.SCHEMA_FILE))
                            continue;
                        LOGGER.info(
                                "Bulk loading... keyspace: {} column family: {}",
                                keyspaceName, columnFamilyName);

                        final String columnFamilyPath = columnFamily.getAbsolutePath();
                        final List<String> command = Arrays.asList(
                                ssTableLoaderBinary, "-d", libProcessAddress, "-u",context.getUsername(),"-pw",context.getPassword(),"-f",
                                cassandraYaml, columnFamilyPath);
                        LOGGER.info("Executing command: {}", command);

                        final ProcessBuilder processBuilder = new ProcessBuilder(
                                command);
                        processBuilder.redirectErrorStream(true);
                        Process process = processBuilder.start();

                        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            LOGGER.info(line);
                        }
                        
                        int exitCode = process.waitFor();
                        LOGGER.info("Command exit code: {}", exitCode);

                        // Send TASK_ERROR
                        if (exitCode != 0) {
                            final String errMessage = String.format(
                                    "Error restoring snapshot. Exit code: %s",
                                    (exitCode + ""));
                            LOGGER.error(errMessage);
                            sendStatus(driver, Protos.TaskState.TASK_ERROR,
                                    errMessage);
                        }

                        LOGGER.info(
                                "Done bulk loading! keyspace: {} column family: {}",
                                keyspaceName, columnFamilyName);
                    }
                    LOGGER.info("Successfully bulk loaded keyspace: {}",
                            keyspaceName);
                }
                // cleanup downloaded snapshot directory recursively.
                Path rootPath = Paths.get(context.getLocalLocation() + File.separator + context.getName());
                if (rootPath.toFile().exists()) {
                    Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                }
            } else {
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
