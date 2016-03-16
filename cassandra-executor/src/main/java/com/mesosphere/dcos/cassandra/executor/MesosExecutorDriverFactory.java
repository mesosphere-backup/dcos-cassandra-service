package com.mesosphere.dcos.cassandra.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;

/**
 * Created by kowens on 2/17/16.
 */
public class MesosExecutorDriverFactory implements ExecutorDriverFactory {

    @Override
    public ExecutorDriver getDriver(Executor executor) {
        return new MesosExecutorDriver(executor);
    }
}
