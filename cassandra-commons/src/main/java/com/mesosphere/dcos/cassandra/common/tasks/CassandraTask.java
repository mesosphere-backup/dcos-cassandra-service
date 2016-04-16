package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.backup.*;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupStatus;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairStatus;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.offer.VolumeRequirement;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.apache.mesos.offer.ResourceUtils.*;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = CassandraTask.class,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CassandraDaemonTask.class,
                name = "CASSANDRA_DAEMON"),
        @JsonSubTypes.Type(value = BackupSnapshotTask.class, name =
                "BACKUP_SNAPSHOT"),
        @JsonSubTypes.Type(value = BackupUploadTask.class, name =
                "BACKUP_UPLOAD"),
        @JsonSubTypes.Type(value = DownloadSnapshotTask.class, name =
                "SNAPSHOT_DOWNLOAD"),
        @JsonSubTypes.Type(value = RestoreSnapshotTask.class, name =
                "SNAPSHOT_RESTORE"),
        @JsonSubTypes.Type(value = CleanupTask.class, name =
                "CLEANUP"),
        @JsonSubTypes.Type(value = RepairTask.class, name =
                "REPAIR"),
})
public abstract class CassandraTask {

    public static Serializer<CassandraTask> JSON_SERIALIZER = new
            Serializer<CassandraTask>() {
                @Override
                public byte[] serialize(CassandraTask value)
                        throws SerializationException {

                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException("Error writing " +
                                "CassandraTask to JSON", ex);
                    }
                }

                @Override
                public CassandraTask deserialize(byte[] bytes)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.readValue(bytes, CassandraTask
                                .class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "CassandraTask from JSON", ex);
                    }
                }
            };


    public enum TYPE {
        CASSANDRA_DAEMON,
        BACKUP_SNAPSHOT,
        BACKUP_UPLOAD,
        SNAPSHOT_DOWNLOAD,
        SNAPSHOT_RESTORE,
        CLEANUP,
        REPAIR
    }

    public static CassandraTask parse(Protos.TaskInfo info)
            throws IOException {

        CassandraProtos.CassandraTaskData data =
                CassandraProtos.CassandraTaskData.parseFrom(info
                        .getData());
        List<Resource> resources = info.getResourcesList();
        String role = resources.get(0).getRole();
        String principal = resources.get(0).getReservation().getPrincipal();

        switch (data.getType()) {

            case CASSANDRA_DAEMON:
                CassandraConfig config =
                        CassandraConfig.parse(data.getConfig());
                return CassandraDaemonTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        config.getCpus(),
                        config.getMemoryMb(),
                        config.getDiskMb(),
                        config,
                        CassandraDaemonStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty(),
                                CassandraMode.STARTING));

            case BACKUP_SNAPSHOT:
                return BackupSnapshotTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        BackupSnapshotStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList(),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey()
                );

            case BACKUP_UPLOAD:
                return BackupUploadTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        BackupUploadStatus.create(Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList(),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey(),
                        data.getLocalLocation()
                );

            case SNAPSHOT_DOWNLOAD:
                return DownloadSnapshotTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        DownloadSnapshotStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey(),
                        data.getLocalLocation()
                );

            case SNAPSHOT_RESTORE:
                return RestoreSnapshotTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        RestoreSnapshotStatus.create(
                                Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getBackupName(),
                        data.getExternalLocation(),
                        data.getS3AccessKey(),
                        data.getS3SecretKey(),
                        data.getLocalLocation()
                );

            case CLEANUP:
                return CleanupTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        CleanupStatus.create(Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList()
                );

            case REPAIR:
                return RepairTask.create(
                        info.getTaskId().getValue(),
                        info.getSlaveId().getValue(),
                        data.getAddress(),
                        CassandraTaskExecutor.parse(info.getExecutor()),
                        info.getName(),
                        role,
                        principal,
                        getReservedCpu(info.getResourcesList(), role,
                                principal),
                        (int) getReservedMem(resources,
                                role,
                                principal),
                        (int) getTotalReservedDisk(resources,
                                role,
                                principal),
                        RepairStatus.create(Protos.TaskState.TASK_STAGING,
                                info.getTaskId().getValue(),
                                info.getSlaveId().getValue(),
                                info.getExecutor().getExecutorId().getValue(),
                                Optional.empty()),
                        data.getKeySpacesList(),
                        data.getColumnFamiliesList()
                );
            default:
                return null;
        }
    }

    public static String uniqueId() {
        return UUID.randomUUID().toString();
    }

    protected final TYPE type;
    protected final String id;
    protected final String slaveId;
    protected final String hostname;
    protected final CassandraTaskExecutor executor;
    protected final String name;
    protected final String role;
    protected final String principal;
    protected final double cpus;
    protected final int memoryMb;
    protected final int diskMb;
    protected final VolumeRequirement.VolumeType diskType;
    protected final CassandraTaskStatus status;

    protected CassandraTask(
            TYPE type,
            String id,
            String slaveId,
            String hostname,
            CassandraTaskExecutor executor,
            String name,
            String role,
            String principal,
            double cpus,
            int memoryMb,
            int diskMb,
            VolumeRequirement.VolumeType diskType,
            CassandraTaskStatus status) {
        this.type = type;
        this.id = id;
        this.slaveId = slaveId;
        this.hostname = hostname;
        this.executor = executor;
        this.name = name;
        this.role = role;
        this.principal = principal;
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.diskMb = diskMb;
        this.diskType = diskType;
        this.status = status;
    }

    @JsonProperty("cpus")
    public double getCpus() {
        return cpus;
    }

    @JsonProperty("disk_mb")
    public int getDiskMb() {
        return diskMb;
    }

    @JsonProperty("disk_type")
    public VolumeRequirement.VolumeType getDiskType() {
        return diskType;
    }

    @JsonProperty("memory_mb")
    public int getMemoryMb() {
        return memoryMb;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("principal")
    public String getPrincipal() {
        return principal;
    }

    @JsonProperty("role")
    public String getRole() {
        return role;
    }

    @JsonProperty("status")
    public CassandraTaskStatus getStatus() {
        return status;
    }

    @JsonProperty("executor")
    public CassandraTaskExecutor getExecutor() {
        return executor;
    }

    @JsonProperty("hostname")
    public String getHostname() {
        return hostname;
    }

    @JsonProperty("slave_id")
    public String getSlaveId() {
        return slaveId;
    }

    @JsonProperty("type")
    public TYPE getType() {
        return type;
    }

    public Protos.TaskInfo toProto() {
        return Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId))
                .setName(name)
                .setData(getTaskData().toByteString())
                .addAllResources(getReserveResources())
                .addAllResources(getCreateResources())
                .addAllResources(getLaunchResources())
                .setExecutor(executor.toExecutorInfo(role, principal))
                .build();
    }

    @JsonIgnore
    public boolean isTerminated() {
        return status.isTerminated();
    }

    @JsonIgnore
    public boolean isRunning() {
        return status.isRunning();
    }

    @JsonIgnore
    public boolean isLaunching() {
        return status.isLaunching();
    }

    @JsonIgnore
    public abstract CassandraTask update(Protos.Offer offer);

    @JsonIgnore
    public abstract CassandraTask updateId(String id);

    @JsonIgnore
    public abstract CassandraProtos.CassandraTaskData getTaskData();

    @JsonIgnore
    public abstract CassandraTask update(CassandraTaskStatus status);

    public abstract CassandraTask update(Protos.TaskState state);

    @JsonIgnore
    public abstract List<Resource> getReserveResources();

    @JsonIgnore
    public abstract List<Resource> getCreateResources();

    @JsonIgnore
    public abstract List<Resource> getLaunchResources();

    @JsonIgnore
    public int getNativeTransportPort() { return -1; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTask)) return false;
        CassandraTask that = (CassandraTask) o;
        return Double.compare(that.getCpus(), getCpus()) == 0 &&
                getMemoryMb() == that.getMemoryMb() &&
                getDiskMb() == that.getDiskMb() &&
                getDiskType() == that.getDiskType() &&
                getType() == that.getType() &&
                Objects.equals(getId(), that.getId()) &&
                Objects.equals(getSlaveId(), that.getSlaveId()) &&
                Objects.equals(getHostname(), that.getHostname()) &&
                Objects.equals(getExecutor(), that.getExecutor()) &&
                Objects.equals(getName(), that.getName()) &&
                Objects.equals(getRole(), that.getRole()) &&
                Objects.equals(getPrincipal(), that.getPrincipal()) &&
                Objects.equals(getStatus(), that.getStatus());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getId(), getSlaveId(), getHostname(),
                getExecutor(), getName(), getRole(), getPrincipal(), getCpus(),
                getMemoryMb(), getDiskMb(), getDiskType(), getStatus());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
