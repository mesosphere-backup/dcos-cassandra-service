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

import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairStatus;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implements anti-entropy, primary range, sequential repair by executing
 * RepairTask by delegating repair to the CassandraDaemonProcess.
 */
public class Repair implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Repair.class);

    private final CassandraDaemonProcess daemon;
    private final ExecutorDriver driver;
    private final RepairTask task;

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

    private void repairKeyspace(String keyspace, List<String> columnFamilies)
            throws
            Exception {
        LOGGER.info("Starting repair : keySpace = {}, columnFamilies = {}",
                keyspace, columnFamilies);

        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PRIMARY_RANGE_KEY, "true");
        options.put(RepairOption.COLUMNFAMILIES_KEY,
                String.join(",", columnFamilies));
        options.put(RepairOption.PARALLELISM_KEY,
                RepairParallelism.SEQUENTIAL.getName());
        options.put(RepairOption.INCREMENTAL_KEY, "false");

        String result = daemon.repair(keyspace, options);

        LOGGER.info("Repair output = {}", result);
        LOGGER.info("Completed repair : keySpace = {}, columnFamilies = {}",
                keyspace, columnFamilies);

        sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                String.format(
                        "Completed repair : keySpace = %s, columnFamilies = %s",
                        keyspace, columnFamilies));
    }

    /**
     * Creates a new Repair.
     * @param driver The ExecutorDriver used to send status updates.
     * @param daemon The CassandraDaemonProcess used to execute the repair.
     * @param task The RepairTask that will be executed.
     */
    public Repair(final ExecutorDriver driver,
                  final CassandraDaemonProcess daemon,
                  final RepairTask task) {
        this.driver = driver;
        this.daemon = daemon;
        this.task = task;
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            final List<String> keySpaces = getKeySpaces();
            final List<String> columnFamilies = getColumnFamilies();
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    String.format("Starting repair: keySpaces = %s, " +
                                    "columnFamilies = %s",
                            keySpaces,
                            columnFamilies));

            for (String keyspace : keySpaces) {
                repairKeyspace(keyspace, columnFamilies);
            }

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    String.format("Completed repair: keySpaces = %s, " +
                                    "columnFamilies = %s",
                            keySpaces,
                            columnFamilies));
        } catch (Throwable t) {
            // Send TASK_FAILED
            LOGGER.error("Repair failed", t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        }
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state, String message) {
        Protos.TaskStatus status = RepairStatus.create(
                state,
                task.getId(),
                task.getSlaveId(),
                task.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }
}
