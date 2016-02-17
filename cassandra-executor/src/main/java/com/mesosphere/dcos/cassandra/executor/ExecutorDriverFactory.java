package com.mesosphere.dcos.cassandra.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;

/**
 * Created by kowens on 2/17/16.
 */
public interface ExecutorDriverFactory {

    ExecutorDriver getDriver(Executor executor);
}
