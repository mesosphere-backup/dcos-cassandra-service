package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = CassandraTaskStatus.class,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CassandraDaemonStatus.class, name =
                "CASSANDRA_DAEMON"),
        @JsonSubTypes.Type(value = BackupSnapshotStatus.class, name =
                "BACKUP_SNAPSHOT"),
        @JsonSubTypes.Type(value = BackupUploadStatus.class, name =
                "BACKUP_UPLOAD"),
        @JsonSubTypes.Type(value = DownloadSnapshotStatus.class, name =
                "SNAPSHOT_DOWNLOAD"),
        @JsonSubTypes.Type(value = RestoreSnapshotStatus.class, name =
                "SNAPSHOT_RESTORE"),
})
public abstract class CassandraTaskStatus {

    public static CassandraTaskStatus parse(Protos.TaskStatus status)
            throws IOException {
        CassandraProtos.CassandraTaskStatusData data =
                CassandraProtos.CassandraTaskStatusData.parseFrom(status
                        .getData());

        switch (data.getType()) {
            case CASSANDRA_DAEMON:
                return CassandraDaemonStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty(),
                        CassandraMode.values()[data.getMode()]);

            case BACKUP_SNAPSHOT:
                return BackupSnapshotStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty());

            case BACKUP_UPLOAD:
                return BackupUploadStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty());

            case SNAPSHOT_DOWNLOAD:
                return DownloadSnapshotStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty()
                );

            case SNAPSHOT_RESTORE:
                return RestoreSnapshotStatus.create(
                        status.getState(),
                        status.getTaskId().getValue(),
                        status.getSlaveId().getValue(),
                        status.getExecutorId().getValue(),
                        (status.hasMessage()) ?
                                Optional.of(
                                        status.getMessage()) :
                                Optional.empty()
                );

            default:
                return null;
        }

    }

    @JsonProperty("state")
    protected final Protos.TaskState state;
    @JsonProperty("type")
    protected final CassandraTask.TYPE type;
    @JsonProperty("slaveId")
    protected final String slaveId;
    @JsonProperty("id")
    protected final String id;
    @JsonProperty("executorId")
    protected final String executorId;
    @JsonProperty("message")
    protected final Optional<String> message;

    protected CassandraTaskStatus(
            CassandraTask.TYPE type,
            Protos.TaskState state,
            String id,
            String slaveId,
            String executorId,
            Optional<String> message) {
        this.type = type;
        this.state = state;
        this.id = id;
        this.slaveId = slaveId;
        this.executorId = executorId;
        this.message = message;
    }

    public Protos.TaskState getState() {
        return state;
    }

    public String getId() {
        return id;
    }

    public String getExecutorId() {
        return executorId;
    }

    public Optional<String> getMessage() {
        return message;
    }

    public String getSlaveId() {
        return slaveId;
    }

    public CassandraTask.TYPE getType() {
        return type;
    }

    @JsonIgnore
    public abstract CassandraTaskStatus update(Protos.TaskState state);

    @JsonIgnore
    protected abstract CassandraProtos.CassandraTaskStatusData getData();

    public Protos.TaskStatus toProto() {
        Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder()
                .setTaskId(
                        Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(
                        Protos.SlaveID.newBuilder().setValue(slaveId))
                .setExecutorId(
                        Protos.ExecutorID.newBuilder().setValue(executorId))
                .setData(getData().toByteString())
                .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
                .setState(state);

        message.map(builder::setMessage);

        return builder.build();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTaskStatus)) return false;
        CassandraTaskStatus that = (CassandraTaskStatus) o;
        return getState() == that.getState() &&
                getType() == that.getType() &&
                Objects.equals(getSlaveId(), that.getSlaveId()) &&
                Objects.equals(getId(), that.getId()) &&
                Objects.equals(getExecutorId(), that.getExecutorId()) &&
                Objects.equals(getMessage(), that.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getState(), getType(), getSlaveId(), getId(),
                getExecutorId(), getMessage());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
