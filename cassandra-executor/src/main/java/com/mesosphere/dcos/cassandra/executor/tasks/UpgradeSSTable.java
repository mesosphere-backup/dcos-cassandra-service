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

import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * Implements the execution of UpgradeSSTableTask for the node invoking the upgradeSSTable
 * methods of the CassandraDaemonProcess for the key spaces and column
 * families indicated by the task.
 */
public class UpgradeSSTable implements ExecutorTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpgradeSSTable.class);

    private final CassandraDaemonProcess daemon;
    private final ExecutorDriver driver;
    private final UpgradeSSTableTask task;

    private List<String> getKeySpaces() {
        if (task.getUpgradeSSTableContext().getKeySpaces().isEmpty()) {
            return daemon.getNonSystemKeySpaces();
        } else {
            return task.getUpgradeSSTableContext().getKeySpaces();
        }
    }

    private List<String> getColumnFamilies() {
        return task.getUpgradeSSTableContext().getColumnFamilies();
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state, String message) {
        Protos.TaskStatus status =
            task.createStatus(state, Optional.of(message)).getTaskStatus();
        driver.sendStatusUpdate(status);
    }

    /**
     * Construct a new UpgradeSSTable.
     *
     * @param driver The ExecutorDriver used to send task status.
     * @param daemon The CassandraDaemonProcess used to cleanup the node.
     * @param task   The UpgradeSSTableTask executed by the UpgradeSSTable.
     */
    public UpgradeSSTable(final ExecutorDriver driver,
                          final CassandraDaemonProcess daemon,
                          final UpgradeSSTableTask task) {
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
                String.format("Starting upgradesstable: keySpaces = %s, " +
                        "columnFamilies = %s",
                    keySpaces,
                    columnFamilies));

            for (String keyspace : keySpaces) {
                LOGGER.info("Starting upgradesstable : keySpace = {}, " +
                        "columnFamilies = {}",
                    keyspace,
                    Arrays.asList(columnFamilies));

                daemon.upgradeSSTables(keyspace, columnFamilies);

                LOGGER.info("Completed upgradesstable : keySpace = {}, " +
                        "columnFamilies = {}",
                    keyspace,
                    Arrays.asList(columnFamilies));
            }

            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                String.format("Completed upgradesstable: keySpaces = %s, " +
                        "columnFamilies = %s",
                    keySpaces,
                    Arrays.asList(columnFamilies)));
        } catch (final Throwable t) {
            LOGGER.error("UpgradeSSTable failed", t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        }
    }

    @Override
    public void stop(Future<?> future) {
        future.cancel(true);
    }
}
