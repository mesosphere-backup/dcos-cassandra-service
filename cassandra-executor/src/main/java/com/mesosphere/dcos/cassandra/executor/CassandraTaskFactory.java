package com.mesosphere.dcos.cassandra.executor;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.apache.mesos.executor.ExecutorTaskException;
import org.apache.mesos.executor.TimedExecutorTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This class generates ExecutorTasks upon request from the CustomScheduler.
 */
public class CassandraTaskFactory implements org.apache.mesos.executor.ExecutorTaskFactory {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Override
    public ExecutorTask createTask(String taskType, Protos.TaskInfo taskInfo, ExecutorDriver driver) throws ExecutorTaskException {
        return null;
    }

    @Override
    public List<TimedExecutorTask> createTimedTasks(String taskType, Protos.ExecutorInfo executorInfo, ExecutorDriver driver) throws ExecutorTaskException {
        LOGGER.info("Received request to createTimedTasks with ExecutorInfo:" + executorInfo);
        return null;
    }
}
