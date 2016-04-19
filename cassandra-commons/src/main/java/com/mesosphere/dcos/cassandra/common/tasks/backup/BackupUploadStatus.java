package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

import java.util.Optional;

public class BackupUploadStatus extends CassandraTaskStatus {
    @JsonCreator
    public static BackupUploadStatus create(
            @JsonProperty("state") Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("executor_id") String executorId,
            @JsonProperty("message") Optional<String> message) {
        return new BackupUploadStatus(state, id, slaveId, executorId, message);
    }

    protected BackupUploadStatus(Protos.TaskState state,
                                 String id,
                                 String slaveId,
                                 String executorId,
                                 Optional<String> message) {
        super(CassandraTask.TYPE.BACKUP_UPLOAD,
                state,
                id,
                slaveId,
                executorId,
                message);
    }

    @Override
    public BackupUploadStatus update(Protos.TaskState state) {
        if (isFinished()) {
            return this;
        } else {
            return create(state, id, slaveId, executorId, message);
        }
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        return CassandraProtos.CassandraTaskStatusData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.BACKUP_UPLOAD)
                .build();
    }
}
