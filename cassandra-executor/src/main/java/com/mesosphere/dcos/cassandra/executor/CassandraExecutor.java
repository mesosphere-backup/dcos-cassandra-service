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
package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.executor.tasks.CassandraTaskFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.CustomExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * CassandraExecutor implements the Executor for the framework. It is
 * responsible for launching the CassandraDaemonProcess and any ClusterTasks
 * that are executed for the Cassandra node.
 * Note that, unless the CassandraDaemonProcess is running, Cluster tasks
 * will not be able to execute.
 */
public class CassandraExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraExecutor.class);

    private final ScheduledExecutorService executor;
    private final ExecutorService clusterJobExecutorService;
    private CassandraTaskFactory cassandraTaskFactory;
    private CustomExecutor customExecutor;

    /**
     * Constructs a new CassandraExecutor
     *
     * @param executor                  The ScheduledExecutorService used by the
     *                                  CassandraDaemonProcess for its watchdog and monitoring
     *                                  tasks.
     * @param clusterJobExecutorService The ExecutorService used by the
     *                                  Executor to run ClusterTasks.
     */
    @Inject
    public CassandraExecutor(final ScheduledExecutorService executor,
                             final ExecutorService clusterJobExecutorService) {
        this.executor = executor;
        this.clusterJobExecutorService = clusterJobExecutorService;
    }


    @Override
    public void registered(ExecutorDriver driver,
                           Protos.ExecutorInfo executorInfo,
                           Protos.FrameworkInfo frameworkInfo,
                           Protos.SlaveInfo slaveInfo) {
        cassandraTaskFactory = new CassandraTaskFactory(driver);
        customExecutor = new CustomExecutor(clusterJobExecutorService, cassandraTaskFactory);
    }

    @Override
    public void reregistered(ExecutorDriver driver,
                             Protos.SlaveInfo slaveInfo) {
        customExecutor.reregistered(driver, slaveInfo);
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        customExecutor.disconnected(driver);
    }

    @Override
    public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {
        customExecutor.launchTask(driver, task);
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        customExecutor.killTask(driver, taskId);
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        customExecutor.frameworkMessage(driver, data);
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        customExecutor.shutdown(driver);
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        customExecutor.error(driver, message);
    }

    public Optional<CassandraDaemonProcess> getCassandraDaemon() {
        CassandraDaemonProcess cassandra = cassandraTaskFactory.getCassandra();
        return (cassandra != null && cassandra.isOpen()) ?
            Optional.of(cassandra) : Optional.empty();
    }
}
