package com.mesosphere.dcos.cassandra.common.tasks.cleanup;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

import java.util.Optional;

public class CleanupStatus extends CassandraTaskStatus{

    @JsonCreator
    public static CleanupStatus create(
            @JsonProperty("state") Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("executor_id") String executorId,
            @JsonProperty("message") Optional<String> message) {
        return new CleanupStatus(state, id, slaveId, executorId, message);
    }

    protected CleanupStatus(Protos.TaskState state,
                                   String id,
                                   String slaveId,
                                   String executorId,
                                   Optional<String> message) {
        super(CassandraTask.TYPE.CLEANUP,
                state,
                id,
                slaveId,
                executorId,
                message);
    }

    @Override
    public CleanupStatus update(Protos.TaskState state) {
        return create(state, id, slaveId, executorId, message);
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        return CassandraProtos.CassandraTaskStatusData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.CLEANUP)
                .build();
    }
}
