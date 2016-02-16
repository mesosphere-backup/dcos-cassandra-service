package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import org.apache.mesos.Protos;

import java.util.Optional;


public class S3BackupStatus extends CassandraTaskStatus {

    @JsonCreator
    public static S3BackupStatus create(
            @JsonProperty("state") Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slaveId") String slaveId,
            @JsonProperty("executorId") String executorId,
            @JsonProperty("message") Optional<String> message) {

        return new S3BackupStatus(state, id, slaveId, executorId, message);
    }

    protected S3BackupStatus(Protos.TaskState state,
                             String id,
                             String slaveId,
                             String executorId,
                             Optional<String> message) {
        super(CassandraTask.TYPE.S3_BACKUP,
                state,
                id,
                slaveId,
                executorId,
                message);
    }

    @Override
    public S3BackupStatus update(Protos.TaskState state) {
        return create(state,id,slaveId,executorId,message);
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        return CassandraProtos.CassandraTaskStatusData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.BACKUP)
                .build();
    }




}
