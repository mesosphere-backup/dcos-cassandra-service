package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

import java.util.Optional;

public class DownloadSnapshotStatus extends CassandraTaskStatus {
    @JsonCreator
    public static DownloadSnapshotStatus create(
            @JsonProperty("state") Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slaveId") String slaveId,
            @JsonProperty("executorId") String executorId,
            @JsonProperty("message") Optional<String> message) {
        return new DownloadSnapshotStatus(state, id, slaveId, executorId, message);
    }

    protected DownloadSnapshotStatus(Protos.TaskState state,
                                     String id,
                                     String slaveId,
                                     String executorId,
                                     Optional<String> message) {
        super(CassandraTask.TYPE.SNAPSHOT_DOWNLOAD,
                state,
                id,
                slaveId,
                executorId,
                message);
    }

    @Override
    public DownloadSnapshotStatus update(Protos.TaskState state) {
        return create(state, id, slaveId, executorId, message);
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        return CassandraProtos.CassandraTaskStatusData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.SNAPSHOT_DOWNLOAD)
                .build();
    }
}
