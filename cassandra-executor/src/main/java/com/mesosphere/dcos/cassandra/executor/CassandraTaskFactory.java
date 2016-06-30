package com.mesosphere.dcos.cassandra.executor;

import com.mesosphere.dcos.cassandra.executor.tasks.UlimitExecutorTask;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.apache.mesos.executor.ExecutorTaskException;
import org.apache.mesos.executor.TimedExecutorTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
        return Arrays.asList(
            new UlimitExecutorTask("-l", getMemlock(executorInfo)), // The maximum size that can be locked into memory.
            new UlimitExecutorTask("-n", getNofile(executorInfo)),  // The maximum number of open file descriptors.
            new UlimitExecutorTask("-u", getNproc(executorInfo)));  // The maximum number of processes available to a single user.
    }

    private String getMemlock(Protos.ExecutorInfo executorInfo) {
        return getEnvironmentValue(executorInfo,"CASSANDRA_ULIMIT_MEMLOCK");
    }

    private String getNofile(Protos.ExecutorInfo executorInfo) {
        return getEnvironmentValue(executorInfo,"CASSANDRA_ULIMIT_NOFILE");
    }

    private String getNproc(Protos.ExecutorInfo executorInfo) {
        return getEnvironmentValue(executorInfo,"CASSANDRA_ULIMIT_NPROC");
    }

    private String getEnvironmentValue(Protos.ExecutorInfo executorInfo, String key) {
        for (Protos.Environment.Variable var : executorInfo.getCommand().getEnvironment().getVariablesList()) {
            if (key.equals(var.getName())) {
               return var.getValue();
            }
        }

        // An empty string as an argument to 'ulimit' has the effect of merely printing the ulimit value.
        return "";
    }
}
