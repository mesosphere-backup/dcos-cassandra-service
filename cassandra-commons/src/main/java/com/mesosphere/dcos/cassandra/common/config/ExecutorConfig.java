package com.mesosphere.dcos.cassandra.common.config;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Objects;

public class ExecutorConfig {

    public static ExecutorConfig create(
            String command,
            List<String> arguments,
            double cpus,
            int memoryMb,
            int heapMb,
            int apiPort,
            String javaHome,
            URI jreLocation,
            URI executorLocation,
            URI cassandraLocation) {

        return new ExecutorConfig(
                command,
                arguments,
                cpus,
                memoryMb,
                heapMb,
                apiPort,
                javaHome,
                jreLocation,
                executorLocation,
                cassandraLocation);
    }

    @JsonCreator
    public static ExecutorConfig create(
            @JsonProperty("command") String command,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("heap_mb") int heapMb,
            @JsonProperty("api_port") int apiPort,
            @JsonProperty("java_home") String javaHome,
            @JsonProperty("jre_location") String jreLocation,
            @JsonProperty("executor_location") String executorLocation,
            @JsonProperty("cassandra_location") String cassandraLocation,
            @JsonProperty("emc_ecs_workaround") boolean emcEcsWorkaround)
            throws URISyntaxException, UnsupportedEncodingException {

        ExecutorConfig config = create(
                command,
                arguments,
                cpus,
                memoryMb,
                heapMb,
                apiPort,
                javaHome,
                URI.create(jreLocation),
                URI.create(executorLocation),
                URI.create(cassandraLocation));

        return config;
    }

    @JsonProperty("command")
    private final String command;

    @JsonProperty("arguments")
    private final List<String> arguments;

    @JsonProperty("cpus")
    private final double cpus;

    @JsonProperty("memory_mb")
    private final int memoryMb;

    @JsonProperty("heap_mb")
    private final int heapMb;

    @JsonProperty("api_port")
    private final int apiPort;

    private final URI jreLocation;
    private final URI executorLocation;
    private final URI cassandraLocation;

    @JsonProperty("java_home")
    private final String javaHome;

    public ExecutorConfig(
            String command,
            List<String> arguments,
            double cpus,
            int memoryMb,
            int heapMb,
            int apiPort,
            String javaHome,
            URI jreLocation,
            URI executorLocation,
            URI cassandraLocation) {

        this.command = command;
        this.arguments = arguments;
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.heapMb = heapMb;
        this.apiPort = apiPort;
        this.jreLocation = jreLocation;
        this.executorLocation = executorLocation;
        this.cassandraLocation = cassandraLocation;
        this.javaHome = javaHome;
    }


    public int getApiPort() {
        return apiPort;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public URI getCassandraLocation() {
        return cassandraLocation;
    }

    public String getCommand() {
        return command;
    }

    public double getCpus() {
        return cpus;
    }

    public URI getExecutorLocation() {
        return executorLocation;
    }

    public int getHeapMb() {
        return heapMb;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public URI getJreLocation() {
        return jreLocation;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    @JsonProperty("jre_location")
    public String getJreLocationString() {
        return jreLocation.toString();
    }

    @JsonProperty("executor_location")
    public String getExecutorLocationString() {
        return executorLocation.toString();
    }

    @JsonProperty("cassandra_location")
    public String getCassandraLocationString() {
        return cassandraLocation.toString();
    }

    @JsonIgnore
    public Set<String> getURIs() {
        Set<String> uris = new HashSet<String>();
        uris.add(executorLocation.toString());
        uris.add(cassandraLocation.toString());
        uris.add(jreLocation.toString());

        return uris;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutorConfig)) return false;
        ExecutorConfig that = (ExecutorConfig) o;
        return Double.compare(that.getCpus(), getCpus()) == 0 &&
                getMemoryMb() == that.getMemoryMb() &&
                getHeapMb() == that.getHeapMb() &&
                getApiPort() == that.getApiPort() &&
                Objects.equals(getCommand(), that.getCommand()) &&
                Objects.equals(getArguments(), that.getArguments()) &&
                Objects.equals(getJreLocation(), that.getJreLocation()) &&
                Objects.equals(getExecutorLocation(),
                        that.getExecutorLocation()) &&
                Objects.equals(getCassandraLocation(),
                        that.getCassandraLocation()) &&
                Objects.equals(getJavaHome(), that.getJavaHome());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCommand(), getArguments(), getCpus(),
                getMemoryMb(),
                getHeapMb(), getApiPort(),
                getJreLocation(), getExecutorLocation(), getCassandraLocation(),
                getJavaHome());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
