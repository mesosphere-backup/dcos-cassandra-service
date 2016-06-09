package com.mesosphere.dcos.cassandra.common.tasks;

import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.backup.TemplateTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.TaskUtils;

import java.util.Arrays;
import java.util.Optional;

/**
 * Created by gabriel on 6/8/16.
 */
public class CassandraTemplateTask extends CassandraTask  {

    protected static Protos.SlaveID EMPTY_SLAVE_ID = Protos.SlaveID
            .newBuilder().setValue("").build();

    protected static Protos.TaskID EMPTY_TASK_ID = Protos.TaskID
            .newBuilder().setValue("").build();

    protected CassandraTemplateTask(Protos.TaskInfo taskInfo) {
        super(taskInfo);
    }

    public static CassandraTemplateTask create(
        String role,
        String principal,
        ClusterTaskConfig clusterTaskConfig) {

        Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                .setTaskId(EMPTY_TASK_ID)
                .setName("cluster_task_template")
                .setSlaveId(EMPTY_SLAVE_ID)
                .setData(CassandraData.createTemplateData().getBytes())
                .addAllResources(Arrays.asList(
                        getCpusResource(role, principal, clusterTaskConfig),
                        getMemResource(role, principal, clusterTaskConfig),
                        getDiskResource(role, principal, clusterTaskConfig)))
                .build();

        taskInfo = TaskUtils.setTransient(taskInfo);

        return new CassandraTemplateTask(taskInfo);
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

    private static Protos.Resource getDiskResource(
        String role,
        String principal,
        ClusterTaskConfig clusterTaskConfig) {
        return getScalar(role, principal, "disk", (double) clusterTaskConfig.getDiskMb());
    }

    private static Protos.Resource getScalar(String role, String principal, String name, Double value) {
        return ResourceUtils.getDesiredScalar(role, principal, name, value);
    }
}
