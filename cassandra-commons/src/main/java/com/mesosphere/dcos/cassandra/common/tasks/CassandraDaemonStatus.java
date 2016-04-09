package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import org.apache.mesos.Protos;

import java.util.Objects;
import java.util.Optional;


public class CassandraDaemonStatus extends CassandraTaskStatus {

    @JsonProperty("mode")
    private final CassandraMode mode;

    @JsonCreator
    public static CassandraDaemonStatus create(
            @JsonProperty("state")Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("executor_id") String executorId,
            @JsonProperty("message") Optional<String> message,
            @JsonProperty("mode")CassandraMode mode
    ) {
        return new CassandraDaemonStatus(state,
                id,
                slaveId,
                executorId,
                message,
                mode);
    }

    public CassandraDaemonStatus(final Protos.TaskState state,
                                 final String id,
                                 final String slaveId,
                                 final String executorId,
                                 final Optional<String> message,
                                 final CassandraMode mode) {
        super(CassandraTask.TYPE.CASSANDRA_DAEMON,
                state,
                id,
                slaveId,
                executorId,
                message);
        this.mode = mode;
    }

    public CassandraMode getMode() {
        return mode;
    }


    @Override
    public CassandraDaemonStatus update(Protos.TaskState state) {
        return create(state,id,slaveId,executorId,message,mode);
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        CassandraProtos.CassandraTaskStatusData.Builder builder =
                CassandraProtos.CassandraTaskStatusData.newBuilder()
                        .setType(CassandraProtos.CassandraTaskData.TYPE
                                .CASSANDRA_DAEMON)
                .setMode(mode.ordinal());

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraDaemonStatus)) return false;
        if (!super.equals(o)) return false;
        CassandraDaemonStatus that = (CassandraDaemonStatus) o;
        return Objects.equals(getMode(), that.getMode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getMode());
    }

}
