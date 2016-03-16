package com.mesosphere.dcos.cassandra.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;

public class MesosExecutorDriverFactory implements ExecutorDriverFactory{

    @Override
    public ExecutorDriver getDriver(Executor executor) {
        return new MesosExecutorDriver(executor);
    }
}
