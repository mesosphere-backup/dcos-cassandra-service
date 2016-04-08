package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.mesos.offer.ResourceUtils.*;
import static org.apache.mesos.protobuf.ResourceBuilder.*;


public class CassandraTaskExecutor {


    public static class Builder {

        private String frameworkId;
        private String id;
        private String command;
        private List<String> arguments;
        private double cpus;
        private int memoryMb;
        private int diskMb;
        private int heapMb;
        private int apiPort;
        private int adminPort;
        private List<URI> uris;
        private String javaHome;

        private Builder(CassandraTaskExecutor executor) {

            this.adminPort = executor.adminPort;
            this.frameworkId = executor.frameworkId;
            this.id = executor.id;
            this.command = executor.command;
            this.arguments = executor.arguments;
            this.cpus = executor.cpus;
            this.memoryMb = executor.memoryMb;
            this.diskMb = executor.diskMb;
            this.heapMb = executor.heapMb;
            this.apiPort = executor.apiPort;
            this.uris = executor.uris;
            this.javaHome = executor.javaHome;

        }

        public int getAdminPort() {
            return adminPort;
        }

        public Builder setAdminPort(int adminPort) {
            this.adminPort = adminPort;
            return this;
        }

        public int getApiPort() {
            return apiPort;
        }

        public Builder setApiPort(int apiPort) {
            this.apiPort = apiPort;
            return this;
        }

        public List<String> getArguments() {
            return arguments;
        }

        public Builder setArguments(List<String> arguments) {
            this.arguments = arguments;
            return this;
        }

        public String getCommand() {
            return command;
        }

        public Builder setCommand(String command) {
            this.command = command;
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

        public String getFrameworkId() {
            return frameworkId;
        }

        public Builder setFrameworkId(String frameworkId) {
            this.frameworkId = frameworkId;
            return this;
        }

        public int getHeapMb() {
            return heapMb;
        }

        public Builder setHeapMb(int heapMb) {
            this.heapMb = heapMb;
            return this;
        }

        public String getId() {
            return id;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public String getJavaHome() {
            return javaHome;
        }

        public Builder setJavaHome(String javaHome) {
            this.javaHome = javaHome;
            return this;
        }

        public int getMemoryMb() {
            return memoryMb;
        }

        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        public List<URI> getUris() {
            return uris;
        }

        public Builder setUris(List<URI> uris) {
            this.uris = uris;
            return this;
        }

        public CassandraTaskExecutor build() {
            return create(
                    frameworkId,
                    id,
                    command,
                    arguments,
                    cpus,
                    memoryMb,
                    diskMb,
                    heapMb,
                    apiPort,
                    adminPort,
                    uris,
                    javaHome);
        }
    }

    public static CassandraTaskExecutor create(
            String frameworkId,
            String id,
            String command,
            List<String> arguments,
            double cpus,
            int memoryMb,
            int diskMb,
            int heapMb,
            int apiPort,
            int adminPort,
            List<URI> uris,
            String javaHome) {

        return new CassandraTaskExecutor(
                frameworkId,
                id,
                command,
                arguments,
                cpus,
                memoryMb,
                diskMb,
                heapMb,
                apiPort,
                adminPort,
                uris,
                javaHome);

    }

    public static final CassandraTaskExecutor parse(Protos.ExecutorInfo info) {

        List<Resource> resources = info.getResourcesList();
        String role = resources.get(0).getRole();
        String principal = resources.get(0).getReservation().getPrincipal();
        Map<String, String> env = info.getCommand().getEnvironment()
                .getVariablesList().stream().collect(Collectors.toMap(
                        variable -> variable.getName(),
                        variable -> variable.getValue()
                ));

        return create(
                info.getFrameworkId().getValue(),
                info.getExecutorId().getValue(),
                info.getCommand().getValue(),
                info.getCommand().getArgumentsList(),
                getReservedCpu(info.getResourcesList(), role,
                        principal),
                (int) getReservedMem(resources,
                        role,
                        principal),
                (int) getTotalReservedDisk(resources,
                        role,
                        principal),
                Integer.parseInt(env.get("JVM_OPTS")
                        .replace("-Xmx", "")
                        .replace("M", "")),
                Integer.parseInt(env.get("EXECUTOR_API_PORT")),
                Integer.parseInt(env.get("EXECUTOR_ADMIN_PORT")),
                info.getCommand().getUrisList().stream().map(uri ->
                        uri.getValue()).map(URI::create).collect(
                        Collectors.toList()),
                env.get("JAVA_HOME"));
    }

    @JsonCreator
    public static CassandraTaskExecutor createJson(
            @JsonProperty("framework_id") String frameworkId,
            @JsonProperty("id") String id,
            @JsonProperty("command") String command,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("disk_mb") int diskMb,
            @JsonProperty("heap_mb") int heapMb,
            @JsonProperty("api_port") int apiPort,
            @JsonProperty("admin_port") int adminPort,
            @JsonProperty("uris") List<String> uris,
            @JsonProperty("java_home") String javaHome) {

        return create(
                frameworkId,
                id,
                command,
                arguments,
                cpus,
                memoryMb,
                diskMb,
                heapMb,
                apiPort,
                adminPort,
                uris.stream().map(URI::create).collect(Collectors.toList()),
                javaHome);

    }

    @JsonProperty("framework_id")
    private final String frameworkId;
    @JsonProperty("id")
    private final String id;
    @JsonProperty("command")
    private final String command;
    @JsonProperty("arguments")
    private final List<String> arguments;
    @JsonProperty("cpus")
    private final double cpus;
    @JsonProperty("memory_mb")
    private final int memoryMb;
    @JsonProperty("disk_mb")
    private final int diskMb;
    @JsonProperty("heap_mb")
    private final int heapMb;
    @JsonProperty("api_port")
    private final int apiPort;
    @JsonProperty("admin_port")
    private final int adminPort;
    private final List<URI> uris;
    @JsonProperty("java_home")
    private final String javaHome;

    public CassandraTaskExecutor(
            String frameworkId,
            String id,
            String command,
            List<String> arguments,
            double cpus,
            int memoryMb,
            int diskMb,
            int heapMb,
            int apiPort,
            int adminPort,
            List<URI> uris,
            String javaHome) {
        this.adminPort = adminPort;
        this.frameworkId = frameworkId;
        this.id = id;
        this.command = command;
        this.arguments = arguments;
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.diskMb = diskMb;
        this.heapMb = heapMb;
        this.apiPort = apiPort;
        this.uris = ImmutableList.copyOf(uris);
        this.javaHome = javaHome;
    }

    @JsonProperty("uris")
    public List<String> getUriStrings() {

        return uris.stream()
                .map(uri -> uri.toString()
                ).collect(Collectors.toList());
    }


    public int getAdminPort() {
        return adminPort;
    }

    public int getApiPort() {
        return apiPort;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public String getCommand() {
        return command;
    }

    public int getDiskMb() {
        return diskMb;
    }

    public double getCpus() {
        return cpus;
    }

    public String getFrameworkId() {
        return frameworkId;
    }

    public int getHeapMb() {
        return heapMb;
    }

    public String getId() {
        return id;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public Builder mutable() {
        return new Builder(this);
    }

    private List<Protos.CommandInfo.URI> getCommandURIs() {

        return getUriStrings().stream().map(uri -> Protos.CommandInfo.URI
                .newBuilder()
                .setValue(uri)
                .setCache(false)
                .setExecutable(false)
                .setExtract(true)
                .build()).collect(Collectors.toList());
    }

    private Protos.Environment getEnvironment() {

        return Protos.Environment.newBuilder()
                .addAllVariables(Arrays.asList(
                        Protos.Environment.Variable
                                .newBuilder().setName("JAVA_HOME")
                                .setValue(javaHome).build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("JVM_OPTS")
                                .setValue("-Xmx" + heapMb + "M").build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("EXECUTOR_API_PORT")
                                .setValue(Integer.toString(apiPort)).build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("EXECUTOR_ADMIN_PORT")
                                .setValue(Integer.toString(adminPort)).build()
                )).build();
    }

    private List<Resource> getResources(String role, String principal) {
        return Arrays.asList(reservedCpus(cpus, role, principal),
                reservedMem(memoryMb, role, principal));
    }

    private Protos.CommandInfo getCommandInfo() {
        return Protos.CommandInfo.newBuilder()
                .setValue(command)
                .addAllArguments(arguments)
                .addAllUris(getCommandURIs())
                .setEnvironment(getEnvironment()).build();
    }

    public Protos.ExecutorInfo toExecutorInfo(String role, String principal) {
        return Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(id))
                .setName(id)
                .addAllResources(getResources(role, principal))
                .setCommand(getCommandInfo())
                .setFrameworkId(
                        Protos.FrameworkID.newBuilder().setValue(frameworkId))
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTaskExecutor)) return false;
        CassandraTaskExecutor that = (CassandraTaskExecutor) o;
        return Double.compare(that.getCpus(), getCpus()) == 0 &&
                getMemoryMb() == that.getMemoryMb() &&
                getDiskMb() == that.getDiskMb() &&
                getHeapMb() == that.getHeapMb() &&
                getApiPort() == that.getApiPort() &&
                getAdminPort() == that.getAdminPort() &&
                Objects.equals(getFrameworkId(), that.getFrameworkId()) &&
                Objects.equals(getId(), that.getId()) &&
                Objects.equals(getCommand(), that.getCommand()) &&
                Objects.equals(getArguments(), that.getArguments()) &&
                Objects.equals(uris, that.uris) &&
                Objects.equals(getJavaHome(), that.getJavaHome());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFrameworkId(), getId(), getCommand(),
                getArguments(),
                getCpus(), getMemoryMb(), getDiskMb(), getHeapMb(),
                getApiPort(),
                getAdminPort(), uris, getJavaHome());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
