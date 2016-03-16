package com.mesosphere.dcos.cassandra.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;

public interface ExecutorDriverFactory {

    ExecutorDriver getDriver(Executor executor);
}
