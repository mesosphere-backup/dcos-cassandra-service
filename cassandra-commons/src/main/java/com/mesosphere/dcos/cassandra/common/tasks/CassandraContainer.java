package com.mesosphere.dcos.cassandra.common.tasks;

import org.apache.mesos.Protos;

import java.util.Arrays;
import java.util.Collection;

/**
 * This class encapsulates a Cassandra Daemon and a Template task that reserves resources for sidecar tasks like
 * Backup, Restore, Upload and Download.
 */
public class CassandraContainer {
    private CassandraDaemonTask daemonTask;
    private CassandraTemplateTask clusterTemplateTask;

    public static CassandraContainer create(
            CassandraDaemonTask daemonTask,
            CassandraTemplateTask clusterTemplateTask) {

        return new CassandraContainer(daemonTask, clusterTemplateTask);
    }

    protected CassandraContainer(CassandraDaemonTask daemonTask, CassandraTemplateTask clusterTemplateTask) {
        this.daemonTask = daemonTask;
        this.clusterTemplateTask = clusterTemplateTask;
    }

    public Collection<Protos.TaskInfo> getTaskInfos() {
        return Arrays.asList(
                Protos.TaskInfo.newBuilder(daemonTask.getTaskInfo()).clearExecutor().build(),
                Protos.TaskInfo.newBuilder(clusterTemplateTask.getTaskInfo()).clearExecutor().build());
    }

    public Protos.ExecutorInfo getExecutorInfo() {
        return daemonTask.getExecutor().getExecutorInfo();
    }

    public Protos.TaskState getState() {
        return daemonTask.getState();
    }

    public CassandraMode getMode() {
        return daemonTask.getMode();
    }

    public boolean isTerminated() {
        return daemonTask.isTerminated();
    }

    public boolean isLaunching() {
        return daemonTask.isLaunching();
    }

    public String getId() {
        return daemonTask.getId();
    }

    public String getAgentId() {
        return daemonTask.getSlaveId();
    }

    public CassandraDaemonTask getDaemonTask() {
        return daemonTask;
    }
}
