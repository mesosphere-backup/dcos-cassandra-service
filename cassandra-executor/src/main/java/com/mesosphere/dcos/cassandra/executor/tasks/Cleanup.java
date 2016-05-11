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

import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupStatus;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Implements the execution of CleanupTask for the node invoking the cleanup
 * methods of the CassandraDaemonProcess for the key spaces and column
 * families indicated by the task.
 */
public class Cleanup implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cleanup.class);

    private final CassandraDaemonProcess daemon;
    private final ExecutorDriver driver;
    private final CleanupTask task;

    private List<String> getKeySpaces() {
        if (task.getKeySpaces().isEmpty()) {
            return daemon.getNonSystemKeySpaces();
        } else {
            return task.getKeySpaces();
        }
    }

    private List<String> getColumnFamilies() {
        return task.getColumnFamilies();
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state, String message) {
        Protos.TaskStatus status = CleanupStatus.create(
                state,
                task.getId(),
                task.getSlaveId(),
                task.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }

    /**
     * Construct a new Cleanup.
     *
     * @param driver The ExecutorDriver used to send task status.
     * @param daemon The CassandraDaemonProcess used to cleanup the node.
     * @param task   The CleanupTask executed by the Cleanup.
     */
    public Cleanup(final ExecutorDriver driver,
                   final CassandraDaemonProcess daemon,
                   final CleanupTask task) {
        this.driver = driver;
        this.daemon = daemon;
        this.task = task;
    }

    @Override
    public void run() {
        try {

            final List<String> keySpaces = getKeySpaces();
            final List<String> columnFamilies = getColumnFamilies();
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    String.format("Starting cleanup: keySpaces = %s, " +
                                    "columnFamilies = %s",
                            keySpaces,
                            columnFamilies));

            for (String keyspace : keySpaces) {
                LOGGER.info("Starting cleanup : keySpace = {}, " +
                                "columnFamilies = {}",
                        keyspace,
                        Arrays.asList(columnFamilies));

                daemon.cleanup(keyspace, columnFamilies);

                LOGGER.info("Completed cleanup : keySpace = {}, " +
                                "columnFamilies = {}",
                        keyspace,
                        Arrays.asList(columnFamilies));
            }

            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    String.format("Completed cleanup: keySpaces = %s, " +
                                    "columnFamilies = %s",
                            keySpaces,
                            Arrays.asList(columnFamilies)));
        } catch (final Throwable t) {
            LOGGER.error("Cleanup failed", t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        }
    }
}
