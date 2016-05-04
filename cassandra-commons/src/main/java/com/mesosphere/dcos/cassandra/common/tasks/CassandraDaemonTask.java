package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.offer.VolumeRequirement;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.mesos.protobuf.ResourceBuilder.*;

public class CassandraDaemonTask extends CassandraTask {

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
        private CassandraDaemonStatus status;
        private CassandraConfig config;

        private Builder(CassandraDaemonTask task) {

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
            this.config = task.config;

        }

        public CassandraConfig getConfig() {
            return config;
        }

        public Builder setConfig(CassandraConfig config) {
            this.config = config;
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

        public CassandraTaskStatus getStatus() {
            return status;
        }

        public Builder setStatus(CassandraDaemonStatus status) {
            this.status = status;
            return this;
        }

        public CassandraDaemonTask build() {
            return create(
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
                    config,
                    status
            );
        }
    }

    public static final String NAME_PREFIX = "node-";

    @JsonCreator
    public static CassandraDaemonTask create(
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("executor") CassandraTaskExecutor executor,
            @JsonProperty("name") String name,
            @JsonProperty("role") String role,
            @JsonProperty("principal") String principal,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("disk_mb") int diskMb,
            @JsonProperty("config") CassandraConfig config,
            @JsonProperty("status") CassandraDaemonStatus status) {

        return new CassandraDaemonTask(
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
                config.getDiskType(),
                config,
                status
        );

    }

    @JsonProperty("config")
    private final CassandraConfig config;

    private final CassandraConfig updateConfig(CassandraDaemonStatus status) {
        if (Protos.TaskState.TASK_RUNNING.equals(status.getState())) {
            return config.mutable().setReplaceIp("").build();
        } else {
            return config;
        }
    }

    protected CassandraDaemonTask(String id,
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
                                  CassandraConfig config,
                                  CassandraDaemonStatus status) {

        super(TYPE.CASSANDRA_DAEMON,
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
                diskType,
                status);

        this.config = config;
    }

    public CassandraConfig getConfig() {
        return config;
    }

    @Override
    public CassandraProtos.CassandraTaskData getTaskData() {
        try {
            return CassandraProtos.CassandraTaskData.newBuilder()
                    .setType(
                            CassandraProtos.CassandraTaskData.TYPE.CASSANDRA_DAEMON)
                    .setConfig(config.toProto())
                    .setAddress(hostname).build();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public CassandraDaemonTask update(CassandraTaskStatus status) {

        if (status.getType() == TYPE.CASSANDRA_DAEMON &&
                status.getId().equals(id)) {

            return create(
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
                    updateConfig((CassandraDaemonStatus) status),
                    (CassandraDaemonStatus) status);
        }
        return this;

    }

    @Override
    public CassandraDaemonTask update(Protos.TaskState state) {


        return create(
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
                config,
                ((CassandraDaemonStatus) status).update(state));

    }

    @Override
    public CassandraDaemonStatus getStatus() {
        return (CassandraDaemonStatus) status;
    }

    public Builder mutable() {
        return new Builder(this);
    }

    @Override
    public CassandraDaemonTask update(Protos.Offer offer) {
        return create(
                id,
                offer.getSlaveId().getValue(),
                offer.getHostname(),
                executor,
                name,
                role,
                principal,
                cpus,
                memoryMb,
                diskMb,
                config,
                CassandraDaemonStatus.create(
                        Protos.TaskState.TASK_STAGING,
                        id,
                        offer.getSlaveId().getValue(),
                        name,
                        Optional.empty(),
                        CassandraMode.STARTING));
    }

    @Override
    public CassandraDaemonTask updateId(String id) {
        return create(
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
                config,
                (CassandraDaemonStatus) status);
    }

    @Override
    public List<Resource> getReserveResources() {
        return Arrays.asList(

                reservedPorts(PortRange.fromPorts(
                        executor.getApiPort(),
                        config.getJmxPort(),
                        config.getApplication().getStoragePort(),
                        config.getApplication().getSslStoragePort(),
                        config.getApplication().getRpcPort(),
                        config.getApplication().getNativeTransportPort())
                        .stream().map(range -> range.toProto()).collect(
                                Collectors.toList()), role, principal));
    }


    @Override
    public List<Resource> getCreateResources() {
        return Arrays.asList(config.getVolume().toResource(role, principal));
    }

    @Override
    public List<Resource> getLaunchResources() {
        return Arrays.asList(reservedCpus(cpus, role, principal),
                reservedMem(memoryMb, role, principal));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraDaemonTask)) return false;
        CassandraDaemonTask that = (CassandraDaemonTask) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }
}
