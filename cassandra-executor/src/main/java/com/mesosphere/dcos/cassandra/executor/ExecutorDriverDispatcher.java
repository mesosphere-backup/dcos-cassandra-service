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
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * The ExecutorDriverDispatcher executes the Mesos Executor in a separate
 * thread of execution so that is asynchronous to the main thread.
 */
public class ExecutorDriverDispatcher implements Runnable, Managed {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ExecutorDriverDispatcher.class);

    private static final int exitCode(final Protos.Status status) {
        return (status == Protos.Status.DRIVER_ABORTED ||
                status == Protos.Status.DRIVER_NOT_STARTED) ?
                -1 : 0;
    }

    private final ExecutorDriver driver;
    private final ExecutorService executor;

    /**
     * Constructs a new ExecutorDriverDispatcher
     * @param driverFactory The ExecutorDriverFactroy that will be used to
     *                      retrieve the ExecutorDriver
     * @param executor The Executor corresponding to the ExecutorDriver.
     * @param executorService The ExecutorService used to execute the
     *                        ExecutorDriver.
     */
    @Inject
    public ExecutorDriverDispatcher(final ExecutorDriverFactory driverFactory,
                                    final Executor executor,
                                    final ExecutorService executorService) {
        this.driver = driverFactory.getDriver(executor);
        this.executor = executorService;
    }

    @Override
    public void run() {

        LOGGER.info("Starting driver execution");
        Protos.Status status = driver.run();
        LOGGER.info("Driver execution complete status = {}", status);
        driver.stop();
        System.exit(exitCode(status));
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting ExecutorDriver");
        executor.submit(this);
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping ExecutorDriver");
        this.driver.stop();
    }
}
