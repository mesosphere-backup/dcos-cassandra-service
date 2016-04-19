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
        private List<URI> uris;
        private String javaHome;
        private final boolean metricsEnable;
        private final String metricsCollector;
        private final String metricsPrefix;
        private final boolean metricsPrefixIncludeHostname;
        private final int metricsFlushPeriod;
        private final String metricsFlushPeriodUnit;
        private final String metricsHost;
        private final int metricsPort;

        private Builder(CassandraTaskExecutor executor) {

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
            this.metricsEnable = executor.metricsEnable;
            this.metricsCollector = executor.metricsCollector;
            this.metricsPrefix = executor.metricsPrefix;
            this.metricsPrefixIncludeHostname =
                    executor.metricsPrefixIncludeHostname;
            this.metricsFlushPeriod = executor.metricsFlushPeriod;
            this.metricsFlushPeriodUnit = executor.metricsFlushPeriodUnit;
            this.metricsHost = executor.metricsHost;
            this.metricsPort = executor.metricsPort;
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
                    uris,
                    javaHome,
                    metricsEnable,
                    metricsCollector,
                    metricsPrefix,
                    metricsPrefixIncludeHostname,
                    metricsFlushPeriod,
                    metricsFlushPeriodUnit,
                    metricsHost,
                    metricsPort);
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
            List<URI> uris,
            String javaHome,
            boolean metricsEnable,
            String metricsCollector,
            String metricsPrefix,
            boolean metricsPrefixIncludeHostname,
            int metricsFlushPeriod,
            String metricsFlushPeriodUnit,
            String metricsHost,
            int metricsPort) {

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
                uris,
                javaHome,
                metricsEnable,
                metricsCollector,
                metricsPrefix,
                metricsPrefixIncludeHostname,
                metricsFlushPeriod,
                metricsFlushPeriodUnit,
                metricsHost,
                metricsPort);

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
                Integer.parseInt(env.get("JAVA_OPTS")
                        .replace("-Xmx", "")
                        .replace("M", "")),
                Integer.parseInt(env.get("EXECUTOR_API_PORT")),
                info.getCommand().getUrisList().stream().map(uri ->
                        uri.getValue()).map(URI::create).collect(
                        Collectors.toList()),
                env.get("JAVA_HOME"),
                Boolean.parseBoolean(env.get("EXECUTOR_METRICS_ENABLE")),
                env.get("EXECUTOR_METRICS_COLLECTOR"),
                env.get("EXECUTOR_METRICS_PREFIX"),
                Boolean.parseBoolean(env.get
                        ("EXECUTOR_METRICS_PREFIX_INCLUDE_HOSTNAME")),
                Integer.parseInt(env.get("EXECUTOR_METRICS_FLUSH_PERIOD")),
                env.get("EXECUTOR_METRICS_FLUSH_PERIOD_UNIT"),
                env.get("EXECUTOR_METRICS_HOST"),
                Integer.parseInt(env.get("EXECUTOR_METRICS_PORT")));
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
            @JsonProperty("uris") List<String> uris,
            @JsonProperty("java_home") String javaHome,
            @JsonProperty("metrics_enable") boolean metricsEnable,
            @JsonProperty("metrics_collector") String metricsCollector,
            @JsonProperty("metrics_prefix") String metricsPrefix,
            @JsonProperty("metrics_prefix_include_hostname") boolean
                    metricsPrefixIncludeHostname,
            @JsonProperty("metrics_flush_period") int metricsFlushPeriod,
            @JsonProperty("metrics_flush_period_unit") String metricsFlushPeriodUnit,
            @JsonProperty("metrics_host") String metricsHost,
            @JsonProperty("metrics_port") int metricsPort) {

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
                uris.stream().map(URI::create).collect(Collectors.toList()),
                javaHome,
                metricsEnable,
                metricsCollector,
                metricsPrefix,
                metricsPrefixIncludeHostname,
                metricsFlushPeriod,
                metricsFlushPeriodUnit,
                metricsHost,
                metricsPort);
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
    private final List<URI> uris;
    @JsonProperty("java_home")
    private final String javaHome;
    @JsonProperty("metrics_enable")
    private final boolean metricsEnable;
    @JsonProperty("metrics_collector")
    private final String metricsCollector;
    @JsonProperty("metrics_prefix")
    private final String metricsPrefix;
    @JsonProperty("metrics_prefix_include_hostname")
    private final boolean metricsPrefixIncludeHostname;
    @JsonProperty("metrics_flush_period")
    private final int metricsFlushPeriod;
    @JsonProperty("metrics_flush_period_unit")
    private final String metricsFlushPeriodUnit;
    @JsonProperty("metrics_host")
    private final String metricsHost;
    @JsonProperty("metrics_port")
    private final int metricsPort;

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
            List<URI> uris,
            String javaHome,
            boolean metricsEnable,
            String metricsCollector,
            String metricsPrefix,
            boolean metricsPrefixIncludeHostname,
            int metricsFlushPeriod,
            String metricsFlushPeriodUnit,
            String metricsHost,
            int metricsPort) {
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
        this.metricsEnable = metricsEnable;
        this.metricsCollector = metricsCollector;
        this.metricsPrefix = metricsPrefix;
        this.metricsPrefixIncludeHostname = metricsPrefixIncludeHostname;
        this.metricsFlushPeriod = metricsFlushPeriod;
        this.metricsFlushPeriodUnit = metricsFlushPeriodUnit;
        this.metricsHost = metricsHost;
        this.metricsPort = metricsPort;
    }

    @JsonProperty("uris")
    public List<String> getUriStrings() {

        return uris.stream()
                .map(uri -> uri.toString()
                ).collect(Collectors.toList());
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

    public boolean isMetricsEnable() { return metricsEnable; }

    public String getMetricsCollector() { return metricsCollector; }

    public String getMetricsPrefix() { return metricsPrefix; }

    public boolean getMetricsPrefixIncludeHostname() {
        return metricsPrefixIncludeHostname;
    }

    public int getMetricsFlushPeriod() { return metricsFlushPeriod; }

    public String getMetricsFlushPeriodUnit() { return metricsFlushPeriodUnit; }

    public String getMetricsHost() { return metricsHost; }

    public int getMetricsPort() { return metricsPort; }

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
                                .newBuilder().setName("JAVA_OPTS")
                                .setValue("-Xmx" + heapMb + "M").build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("EXECUTOR_API_PORT")
                                .setValue(Integer.toString(apiPort)).build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("EXECUTOR_METRICS_ENABLE")
                                .setValue(Boolean.toString(metricsEnable))
                                .build(),
                        Protos.Environment.Variable
                                .newBuilder()
                                .setName("EXECUTOR_METRICS_COLLECTOR")
                                .setValue(metricsCollector).build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("EXECUTOR_METRICS_PREFIX")
                                .setValue(metricsPrefix).build(),
                        Protos.Environment.Variable.newBuilder()
                                .setName("EXECUTOR_METRICS_PREFIX_INCLUDE_HOSTNAME")
                                .setValue(Boolean.toString(
                                        metricsPrefixIncludeHostname)).build(),
                        Protos.Environment.Variable
                                .newBuilder()
                                .setName("EXECUTOR_METRICS_FLUSH_PERIOD")
                                .setValue(Integer.toString(metricsFlushPeriod))
                                .build(),
                        Protos.Environment.Variable
                                .newBuilder()
                                .setName("EXECUTOR_METRICS_FLUSH_PERIOD_UNIT")
                                .setValue(metricsFlushPeriodUnit)
                                .build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("EXECUTOR_METRICS_HOST")
                                .setValue(metricsHost)
                                .build(),
                        Protos.Environment.Variable
                                .newBuilder().setName("EXECUTOR_METRICS_PORT")
                                .setValue(Integer.toString(metricsPort))
                                .build()
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
                Objects.equals(getFrameworkId(), that.getFrameworkId()) &&
                Objects.equals(getId(), that.getId()) &&
                Objects.equals(getCommand(), that.getCommand()) &&
                Objects.equals(getArguments(), that.getArguments()) &&
                Objects.equals(uris, that.uris) &&
                Objects.equals(getJavaHome(), that.getJavaHome()) &&
                isMetricsEnable() == that.isMetricsEnable() &&
                Objects.equals(getMetricsCollector(),
                        that.getMetricsCollector()) &&
                Objects.equals(getMetricsPrefix(), that.getMetricsPrefix()) &&
                getMetricsPrefixIncludeHostname() ==
                        that.getMetricsPrefixIncludeHostname() &&
                getMetricsFlushPeriod() == that.getMetricsFlushPeriod() &&
                Objects.equals(getMetricsFlushPeriodUnit(),
                        that.getMetricsFlushPeriodUnit()) &&
                Objects.equals(getMetricsHost(), that.getMetricsHost()) &&
                getMetricsPort() == that.getMetricsPort();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFrameworkId(), getId(), getCommand(),
                getArguments(), getCpus(), getMemoryMb(), getDiskMb(),
                getHeapMb(), getApiPort(), uris, getJavaHome(),
                isMetricsEnable(), getMetricsCollector(), getMetricsPrefix(),
                getMetricsPrefixIncludeHostname(), getMetricsFlushPeriod(),
                getMetricsFlushPeriodUnit(), getMetricsHost(), getMetricsPort());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
