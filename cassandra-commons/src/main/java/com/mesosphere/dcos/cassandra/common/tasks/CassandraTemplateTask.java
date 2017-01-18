package com.mesosphere.dcos.cassandra.common.tasks;

import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.backup.TemplateTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.TaskUtils;

import java.util.Arrays;
import java.util.Optional;


public class CassandraTemplateTask extends CassandraTask  {
    private static final String CLUSTER_TASK_TEMPLATE_SUFFIX = "-task-template";

    public static String toTemplateTaskName(String daemonTaskName) {
        return daemonTaskName + CLUSTER_TASK_TEMPLATE_SUFFIX;
    }

    protected static Protos.SlaveID EMPTY_SLAVE_ID = Protos.SlaveID
            .newBuilder().setValue("").build();

    protected static Protos.TaskID EMPTY_TASK_ID = Protos.TaskID
            .newBuilder().setValue("").build();

    protected CassandraTemplateTask(Protos.TaskInfo taskInfo) {
        super(taskInfo);
    }

    public static CassandraTemplateTask create(
            CassandraDaemonTask daemonTask,
            ClusterTaskConfig clusterTaskConfig) {
        final String role = daemonTask.getExecutor().getRole();
        final String principal = daemonTask.getExecutor().getPrincipal();

        Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                .setTaskId(EMPTY_TASK_ID)
                .setName(toTemplateTaskName(daemonTask.getName()))
                .setSlaveId(EMPTY_SLAVE_ID)
                .setData(CassandraData.createTemplateData().getBytes())
                .addAllResources(Arrays.asList(
                        getCpusResource(role, principal, clusterTaskConfig),
                        getMemResource(role, principal, clusterTaskConfig)))
                .build();

        taskInfo = TaskUtils.setTransient(taskInfo);

        return new CassandraTemplateTask(taskInfo);
    }

    public static CassandraTemplateTask parse(final Protos.TaskInfo info) {
        return new CassandraTemplateTask(info);
    }

    @Override
    public CassandraTask update(Protos.Offer offer) {
        return this;
    }

    @Override
    public CassandraTask updateId() {
        return this;
    }

    @Override
    public CassandraTask update(CassandraTaskStatus status) {
        return this;
    }

    @Override
    public CassandraTask update(Protos.TaskState state) {
        return this;
    }

    @Override
    public CassandraTaskStatus createStatus(Protos.TaskState state, Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return TemplateTaskStatus.create(builder
                .setData(CassandraData.createTemplateData().getBytes())
                .setState(state)
                .build());
    }

    private static Protos.Resource getCpusResource(
            String role,
            String principal,
            ClusterTaskConfig clusterTaskConfig) {
        return getScalar(role, principal, "cpus", clusterTaskConfig.getCpus());
    }

    private static Protos.Resource getMemResource(
        String role,
        String principal,
        ClusterTaskConfig clusterTaskConfig) {
        return getScalar(role, principal, "mem", (double) clusterTaskConfig.getMemoryMb());
    }

    private static Protos.Resource getScalar(String role, String principal, String name, Double value) {
        return ResourceUtils.getDesiredScalar(role, principal, name, value);
    }
}
