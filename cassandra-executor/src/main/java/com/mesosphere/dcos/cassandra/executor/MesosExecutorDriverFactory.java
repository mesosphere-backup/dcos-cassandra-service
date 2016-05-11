/**
 * Creates a new ExecutorModule given the application configuration.
 * @param configuration The application Configuration used to inject
 *                      dependencies.
 * @return The ExecutorModule constructed using configuration.
 */
package com.mesosphere.dcos.cassandra.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;

/**
 * Implements ExecutorDriverFactory to construct MesosExecutorDrivers.
 */
public class MesosExecutorDriverFactory implements ExecutorDriverFactory {

    @Override
    public ExecutorDriver getDriver(Executor executor) {
        return new MesosExecutorDriver(executor);
    }
}
