package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.mesos.protobuf.ResourceBuilder.*;

public class BackupSnapshotTask extends CassandraTask {
    public static final String NAME_PREFIX = "snapshot-";

    public static class Builder {

        private String id;
        private String slaveId;
        private String hostname;
        private CassandraTaskExecutor executor;
        private String name;
        private String role;
        private String principal;
        private double cpus;
        private int memoryMb;
        private int diskMb;
        private BackupSnapshotStatus status;
        private List<String> keySpaces;
        private List<String> columnFamilies;
        private String backupName;
        private String externalLocation;
        private String s3AccessKey;
        private String s3SecretKey;

        private Builder(BackupSnapshotTask task) {
            this.id = task.id;
            this.slaveId = task.slaveId;
            this.hostname = task.hostname;
            this.executor = task.executor;
            this.name = task.name;
            this.role = task.role;
            this.principal = task.principal;
            this.cpus = task.cpus;
            this.memoryMb = task.memoryMb;
            this.diskMb = task.diskMb;
            this.status = task.getStatus();
            this.columnFamilies = task.columnFamilies;
            this.keySpaces = task.keySpaces;
            this.backupName = task.backupName;
            this.externalLocation = task.externalLocation;
            this.s3AccessKey = task.s3AccessKey;
            this.s3SecretKey = task.s3SecretKey;
        }

        public List<String> getColumnFamilies() {
            return columnFamilies;
        }

        public Builder setColumnFamilies(List<String> columnFamilies) {
            this.columnFamilies = columnFamilies;
            return this;
        }

        public double getCpus() {
            return cpus;
        }

        public Builder setCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        public int getDiskMb() {
            return diskMb;
        }

        public Builder setDiskMb(int diskMb) {
            this.diskMb = diskMb;
            return this;
        }

        public CassandraTaskExecutor getExecutor() {
            return executor;
        }

        public Builder setExecutor(CassandraTaskExecutor executor) {
            this.executor = executor;
            return this;
        }

        public String getExternalLocation() {
            return externalLocation;
        }

        public Builder setExternalLocation(String externalLocation) {
            this.externalLocation = externalLocation;
            return this;
        }

        public String getHostname() {
            return hostname;
        }

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public String getId() {
            return id;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public List<String> getKeySpaces() {
            return keySpaces;
        }

        public Builder setKeySpaces(List<String> keySpaces) {
            this.keySpaces = keySpaces;
            return this;
        }

        public int getMemoryMb() {
            return memoryMb;
        }

        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        public String getName() {
            return name;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public String getPrincipal() {
            return principal;
        }

        public Builder setPrincipal(String principal) {
            this.principal = principal;
            return this;
        }

        public String getRole() {
            return role;
        }

        public Builder setRole(String role) {
            this.role = role;
            return this;
        }

        public String getS3AccessKey() {
            return s3AccessKey;
        }

        public Builder setS3AccessKey(String s3AccessKey) {
            this.s3AccessKey = s3AccessKey;
            return this;
        }

        public String getS3SecretKey() {
            return s3SecretKey;
        }

        public Builder setS3SecretKey(String s3SecretKey) {
            this.s3SecretKey = s3SecretKey;
            return this;
        }

        public String getSlaveId() {
            return slaveId;
        }

        public Builder setSlaveId(String slaveId) {
            this.slaveId = slaveId;
            return this;
        }

        public BackupSnapshotStatus getStatus() {
            return status;
        }

        public Builder setStatus(BackupSnapshotStatus status) {
            this.status = status;
            return this;
        }

        public BackupSnapshotTask build() {

            return create(id,
                    slaveId,
                    hostname,
                    executor,
                    name,
                    role,
                    principal,
                    cpus,
                    memoryMb,
                    diskMb,
                    status,
                    keySpaces,
                    columnFamilies,
                    backupName,
                    externalLocation,
                    s3AccessKey,
                    s3SecretKey);
        }
    }

    @JsonProperty("keySpaces")
    private final List<String> keySpaces;

    @JsonProperty("columnFamilies")
    private final List<String> columnFamilies;

    @JsonProperty("backupName")
    private final String backupName;

    @JsonProperty("externalLocation")
    private final String externalLocation;

    @JsonProperty("s3AccessKey")
    private final String s3AccessKey;

    @JsonProperty("s3SecretKey")
    private final String s3SecretKey;

    @JsonCreator
    public static BackupSnapshotTask create(
            @JsonProperty("id") String id,
            @JsonProperty("slaveId") String slaveId,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("executor") CassandraTaskExecutor executor,
            @JsonProperty("name") String name,
            @JsonProperty("role") String role,
            @JsonProperty("principal") String principal,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memoryMb") int memoryMb,
            @JsonProperty("diskMb") int diskMb,
            @JsonProperty("status") BackupSnapshotStatus status,
            @JsonProperty("keySpaces") List<String> keySpaces,
            @JsonProperty("columnFamilies") List<String> columnFamilies,
            @JsonProperty("backupName") String backupName,
            @JsonProperty("externalLocation") String externalLocation,
            @JsonProperty("s3AccessKey") String s3AccessKey,
            @JsonProperty("s3SecretKey") String s3SecretKey) {


        return new BackupSnapshotTask(id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                status,
                keySpaces,
                columnFamilies,
                backupName,
                externalLocation,
                s3AccessKey,
                s3SecretKey);
    }

    protected BackupSnapshotTask(
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
            BackupSnapshotStatus status,
            List<String> keySpaces,
            List<String> columnFamilies,
            String backupName,
            String externalLocation,
            String s3AccessKey,
            String s3SecretKey) {
        super(TYPE.BACKUP_SNAPSHOT,
                id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                status);

        this.keySpaces = ImmutableList.copyOf(keySpaces);
        this.columnFamilies = ImmutableList.copyOf(columnFamilies);
        this.backupName = backupName;
        this.externalLocation = externalLocation;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
    }

    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    public String getBackupName() {
        return backupName;
    }

    public String getExternalLocation() {
        return externalLocation;
    }

    public List<String> getKeySpaces() {
        return keySpaces;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    @Override
    public CassandraProtos.CassandraTaskData getTaskData() {
        return CassandraProtos.CassandraTaskData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.BACKUP_SNAPSHOT)
                .addAllColumnFamilies(columnFamilies)
                .addAllKeySpaces(keySpaces)
                .setBackupName(backupName)
                .setExternalLocation(externalLocation)
                .setS3AccessKey(s3AccessKey)
                .setS3SecretKey(s3SecretKey)
                .build();
    }

    @Override
    public BackupSnapshotTask update(Protos.Offer offer) {
        return create(id,
                offer.getSlaveId().getValue(),
                offer.getHostname(),
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                (BackupSnapshotStatus) status,
                keySpaces,
                columnFamilies,
                backupName,
                externalLocation,
                s3AccessKey,
                s3SecretKey);
    }

    @Override
    public BackupSnapshotTask update(Protos.TaskState state) {
        return create(id,
                slaveId,
                hostname,
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                ((BackupSnapshotStatus) status).update(state),
                keySpaces,
                columnFamilies,
                backupName,
                externalLocation,
                s3AccessKey,
                s3SecretKey);

    }

    @Override
    public BackupSnapshotTask update(CassandraTaskStatus status) {

        if (status.getType() == TYPE.BACKUP_SNAPSHOT &&
                status.getId().equals(id)) {

            return create(id,
                    slaveId,
                    hostname,
                    executor,
                    name,
                    role,
                    principal,
                    cpus,
                    memoryMb,
                    diskMb,
                    (BackupSnapshotStatus) status,
                    keySpaces,
                    columnFamilies,
                    backupName,
                    externalLocation,
                    s3AccessKey,
                    s3SecretKey);
        } else {
            return this;
        }
    }

    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public BackupSnapshotStatus getStatus() {

        return (BackupSnapshotStatus) status;
    }

    @Override
    public List<Protos.Resource> getReserveResources() {
        return Collections.emptyList();
    }

    @Override
    public List<Protos.Resource> getCreateResources() {
        return Collections.emptyList();
    }

    @Override
    public List<Protos.Resource> getLaunchResources() {
        return Arrays.asList(
                reservedCpus(cpus, role, principal),
                reservedMem(memoryMb, role, principal),
                reservedDisk(diskMb, role, principal));
    }
}
