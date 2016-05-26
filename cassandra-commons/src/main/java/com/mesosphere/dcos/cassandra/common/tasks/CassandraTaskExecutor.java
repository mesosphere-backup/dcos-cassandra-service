/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import static org.apache.mesos.protobuf.ResourceBuilder.reservedCpus;
import static org.apache.mesos.protobuf.ResourceBuilder.reservedMem;

/**
 * CassandraTaskExecutor aggregates the executor information for CassandraTasks.
 * It is associated with CassandraTasks prior to launching them. Only one
 * executor should exist on a slave at a time. This executor should be
 * created when launching the CassandraDaemonTask on that slave, and it
 * should be reused by Cluster tasks that operate on the Daemon.
 */
public class CassandraTaskExecutor {

    /**
     * Builder is used for fluent style construction of CassandraTaskExecutor.
     */
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

        /**
         * Gets the port that the executor's api will run on.
         *
         * @return The port that the executor's api will listen on.
         */
        public int getApiPort() {
            return apiPort;
        }

        /**
         * Sets the executor's api port.
         *
         * @param apiPort The port that the executor's api will listen on.
         * @return The Builder instance.
         */
        public Builder setApiPort(int apiPort) {
            this.apiPort = apiPort;
            return this;
        }

        /**
         * The arguments passed to the executor.
         *
         * @return The command line arguments passed to the executor.
         */
        public List<String> getArguments() {
            return arguments;
        }

        /**
         * Sets the arguments passed to the executor.
         *
         * @param arguments The arguments passed to the executor.
         * @return The Builder instance.
         */
        public Builder setArguments(List<String> arguments) {
            this.arguments = arguments;
            return this;
        }

        /**
         * Gets the command used to launch the executor.
         *
         * @return The command used to launch the executor.
         */
        public String getCommand() {
            return command;
        }

        /**
         * Sets the command used to launch the executor.
         *
         * @param command The command used to launch the executor.
         * @return The Builder instance.
         */
        public Builder setCommand(String command) {
            this.command = command;
            return this;
        }

        /**
         * Gets the cpu shares for the executor.
         *
         * @return The cpu shares for the executor.
         */
        public double getCpus() {
            return cpus;
        }

        /**
         * Sets the cpu shares for the executor.
         *
         * @param cpus The cpu shares for the executor.
         * @return The Builder instance.
         */
        public Builder setCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        /**
         * Gets the disk for the executor.
         *
         * @return The size of the disk allocated to the executor in Mb.
         */
        public int getDiskMb() {
            return diskMb;
        }

        /**
         * Sets the disk for the executor.
         *
         * @param diskMb The size of the disk allocated to the executor in Mb.
         * @return The Builder instance.
         */
        public Builder setDiskMb(int diskMb) {
            this.diskMb = diskMb;
            return this;
        }

        /**
         * Gets the framework id for the executor.
         *
         * @return The framework id of the executor.
         */
        public String getFrameworkId() {
            return frameworkId;
        }

        /**
         * Sets the framework id for the executor.
         *
         * @param frameworkId The framework id of the executor.
         * @return The Builder instance.
         */
        public Builder setFrameworkId(String frameworkId) {
            this.frameworkId = frameworkId;
            return this;
        }

        /**
         * Gets the heap allocated to the executor in Mb.
         *
         * @return The heap allocated to the executor's JVM in Mb.
         */
        public int getHeapMb() {
            return heapMb;
        }

        /**
         * Sets the heap allocated to the executor in Mb.
         *
         * @param heapMb The heap allocated to the executor in Mb.
         * @return The Builder instance.
         */
        public Builder setHeapMb(int heapMb) {
            this.heapMb = heapMb;
            return this;
        }

        /**
         * Gets the id of the executor.
         *
         * @return The universally unique identifier for the executor.
         */
        public String getId() {
            return id;
        }

        /**
         * Sets the executor id.
         *
         * @param id The universally unique identifier for the executor.
         * @return The Builder instance.
         */
        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        /**
         * Gets the Java home setting for the executor.
         *
         * @return The location of the local java installation for the executor.
         */
        public String getJavaHome() {
            return javaHome;
        }

        /**
         * Sets the Java home for the executor.
         *
         * @param javaHome The location of the local java installation for
         *                 the executor.
         * @return The Builder instance.
         */
        public Builder setJavaHome(String javaHome) {
            this.javaHome = javaHome;
            return this;
        }

        /**
         * Gets the memory allocated to the executor.
         *
         * @return The memory allocated to the executor in Mb.
         */
        public int getMemoryMb() {
            return memoryMb;
        }

        /**
         * Sets the memory allocated to the executor in Mb.
         *
         * @param memoryMb The memory allocated to the executor in Mb.
         * @return The Builder instance.
         */
        public Builder setMemoryMb(int memoryMb) {
            this.memoryMb = memoryMb;
            return this;
        }

        /**
         * Gets the List of resource URIs associated with the executor.
         *
         * @return The List of resource URIs for the executor.
         */
        public List<URI> getUris() {
            return uris;
        }

        /**
         * Sets the resource URIs associated with the executor.
         *
         * @param uris The resource URIs associated with the executor.
         * @return The Builder instance.
         */
        public Builder setUris(List<URI> uris) {
            this.uris = uris;
            return this;
        }

        /**
         * Creates a CassandraTaskExecutor from the builders properties.
         *
         * @return A CassandraTaskExecutor constructed from the builders
         * properties.
         */
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

    /**
     * Creates a new CassandraTaskExecutor.
     *
     * @param frameworkId The id of the executor's framework.
     * @param id          The executor's universally unique identifier.
     * @param command     The command used to launch the executor.
     * @param arguments   The arguments passed to the executor.
     * @param cpus        The cpu shares allocated to the executor.
     * @param memoryMb    The memory allocated to the executor in Mb.
     * @param diskMb      The disk allocated to the executor in Mb.
     * @param heapMb      The heap allocated to the executor in Mb.
     * @param apiPort     The port the executor's API will listen on.
     * @param uris        The URI's for the executor's resources.
     * @param javaHome    The location of the local java installation for the
     *                    executor.
     * @return A new CassandraTaskExecutor constructed from the parameters.
     */
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

    /**
     * Parses a CassandraTaskExecutor from a Protocol Buffers representation.
     *
     * @param info The ExecutorInfo that contains a CassandraTaskExecutor.
     * @return A CassandraTaskExecutor parsed from info.
     */
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

    /**
     * Creates a new CassandraTaskExecutor. This function is used for the
     * JSON creator to deal with construction of the resource URIs.
     *
     * @param frameworkId The id of the executor's framework.
     * @param id          The executor's universally unique identifier.
     * @param command     The command used to launch the executor.
     * @param arguments   The arguments passed to the executor.
     * @param cpus        The cpu shares allocated to the executor.
     * @param memoryMb    The memory allocated to the executor in Mb.
     * @param diskMb      The disk allocated to the executor in Mb.
     * @param heapMb      The heap allocated to the executor in Mb.
     * @param apiPort     The port the executor's API will listen on.
     * @param uris        The URI's for the executor's resources.
     * @param javaHome    The location of the local java installation for the
     *                    executor.
     * @return A new CassandraTaskExecutor constructed from the parameters.
     */
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


    /**
     * Constructs a CassandraTaskExecutor.
     *
     * @param frameworkId The id of the executor's framework.
     * @param id          The executor's universally unique identifier.
     * @param command     The command used to launch the executor.
     * @param arguments   The arguments passed to the executor.
     * @param cpus        The cpu shares allocated to the executor.
     * @param memoryMb    The memory allocated to the executor in Mb.
     * @param diskMb      The disk allocated to the executor in Mb.
     * @param heapMb      The heap allocated to the executor in Mb.
     * @param apiPort     The port the executor's API will listen on.
     * @param uris        The URI's for the executor's resources.
     * @param javaHome    The location of the local java installation for the
     *                    executor.
     */
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

    /**
     * Gets a String representation of the executor's resource URIs.
     *
     * @return A String representation of the executor's resource URIs.
     */
    @JsonProperty("uris")
    public List<String> getUriStrings() {

        return uris.stream()
                .map(uri -> uri.toString()
                ).collect(Collectors.toList());
    }

    /**
     * Gets the API port.
     *
     * @return The port the executor's API will listen on.
     */
    public int getApiPort() {
        return apiPort;
    }

    /**
     * Gets the executor's arguments.
     *
     * @return The arguments passed to the executor.
     */
    public List<String> getArguments() {
        return arguments;
    }

    /**
     * Gets the command.
     *
     * @return The command used to launch the executor.
     */
    public String getCommand() {
        return command;
    }

    /**
     * Gets the disk allocation.
     *
     * @return The disk allocated to the executor in Mb.
     */
    public int getDiskMb() {
        return diskMb;
    }

    /**
     * Gets the cpu shares.
     *
     * @return The cpu shares allocated to the executor.
     */
    public double getCpus() {
        return cpus;
    }

    /**
     * Gets the framework id.
     *
     * @return The unique id of the executors framework.
     */
    public String getFrameworkId() {
        return frameworkId;
    }

    /**
     * Gets the heap size.
     *
     * @return The size of the executor's JVM heap in Mb.
     */
    public int getHeapMb() {
        return heapMb;
    }

    /**
     * Gets the executors id.
     *
     * @return The universally unique identifier for the executor.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the Java home.
     *
     * @return The location of the executor's local Java installation.
     */
    public String getJavaHome() {
        return javaHome;
    }

    /**
     * Gets the executors memory allocation.
     *
     * @return The memory allocated to the executor in Mb.
     */
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

    /**
     * Gets a mutable Builder.
     * @return A Builder whose properties are set to the properties of the
     * CassandraTaskExecutor.
     */
    public Builder mutable() {
        return new Builder(this);
    }

    /**
     * Gets a Protocol Buffers representation of the executor.
     * @param role The role of the framework.
     * @param principal The framework's principal.
     * @return An ExecutorInfo constructed from the CassandraTaskExecutor
     * using the provided role and principal.
     */
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
