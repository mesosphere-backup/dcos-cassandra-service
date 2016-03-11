package com.mesosphere.dcos.cassandra.common.tasks.cleanup;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.mesos.protobuf.ResourceBuilder.*;

public class CleanupTask extends CassandraTask {
    public static final String NAME_PREFIX = "cleanup-";

    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

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
        private CleanupStatus status;
        private List<String> keySpaces;
        private List<String> columnFamilies;

        private Builder(CleanupTask task) {

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


        public String getSlaveId() {
            return slaveId;
        }

        public Builder setSlaveId(String slaveId) {
            this.slaveId = slaveId;
            return this;
        }

        public CleanupStatus getStatus() {
            return status;
        }

        public Builder setStatus(CleanupStatus status) {
            this.status = status;
            return this;
        }

        public CleanupTask build() {
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
                    columnFamilies);
        }
    }

    @JsonProperty("keySpaces")
    private final List<String> keySpaces;

    @JsonProperty("columnFamilies")
    private final List<String> columnFamilies;


    @JsonCreator
    public static CleanupTask create(
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
            @JsonProperty("status") CleanupStatus status,
            @JsonProperty("keySpaces") List<String> keySpaces,
            @JsonProperty("columnFamilies") List<String> columnFamilies) {
        return new CleanupTask(id,
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
                columnFamilies);
    }

    protected CleanupTask(
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
            CleanupStatus status,
            List<String> keySpaces,
            List<String> columnFamilies) {
        super(CassandraTask.TYPE.CLEANUP,
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
    }

    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    public List<String> getKeySpaces() {
        return keySpaces;
    }


    @Override
    public CassandraProtos.CassandraTaskData getTaskData() {
        return CassandraProtos.CassandraTaskData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.CLEANUP)
                .addAllColumnFamilies(columnFamilies)
                .addAllKeySpaces(keySpaces)
                .build();
    }

    @Override
    public CleanupTask update(Protos.Offer offer) {
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
                (CleanupStatus) status,
                keySpaces,
                columnFamilies);
    }

    @Override
    public CassandraTask updateId(String id) {
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
                (CleanupStatus) status,
                keySpaces,
                columnFamilies);
    }

    @Override
    public CleanupTask update(Protos.TaskState state) {
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
                ((CleanupStatus) status).update(state),
                keySpaces,
                columnFamilies);
    }

    @Override
    public CleanupTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.CLEANUP &&
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
                    (CleanupStatus) status,
                    keySpaces,
                    columnFamilies);
        } else {
            return this;
        }
    }

    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public CleanupStatus getStatus() {
        return (CleanupStatus) status;
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
