package com.mesosphere.dcos.cassandra.common.tasks;

import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.TaskUtils;

import java.util.Arrays;

/**
 * Created by gabriel on 6/8/16.
 */
public class CassandraTemplateTask {

    private String role;
    private String principal;
    private ClusterTaskConfig clusterTaskConfig;
    private Protos.TaskInfo taskInfo;

    protected static Protos.SlaveID EMPTY_SLAVE_ID = Protos.SlaveID
            .newBuilder().setValue("").build();

    protected static Protos.TaskID EMPTY_TASK_ID = Protos.TaskID
            .newBuilder().setValue("").build();

    protected CassandraTemplateTask(
            String role,
            String principal,
            ClusterTaskConfig clusterTaskConfig) {

        this.role = role;
        this.principal = principal;
        this.clusterTaskConfig = clusterTaskConfig;

        taskInfo = Protos.TaskInfo.newBuilder()
                .setTaskId(EMPTY_TASK_ID)
                .setName("cluster_task_template")
                .setSlaveId(EMPTY_SLAVE_ID)
                .addAllResources(Arrays.asList(
                        getCpus(),
                        getMem(),
                        getDisk()))
                .build();

        taskInfo = TaskUtils.setTransient(taskInfo);
    }

    public static CassandraTemplateTask create(
        String role,
        String principal,
        ClusterTaskConfig clusterTaskConfig) {
        return new CassandraTemplateTask(role, principal, clusterTaskConfig);
    }

    public Protos.TaskInfo getTaskInfo() {
        return taskInfo;
    }

    private Protos.Resource getCpus() {
        return getScalar("cpus", clusterTaskConfig.getCpus());
    }

    private Protos.Resource getMem() {
        return getScalar("mem", (double) clusterTaskConfig.getMemoryMb());
    }

    private Protos.Resource getDisk() {
        return getScalar("disk", (double) clusterTaskConfig.getDiskMb());
    }

    private Protos.Resource getScalar(String name, Double value) {
        return ResourceUtils.getDesiredScalar(role, principal, name, value);
    }
}
