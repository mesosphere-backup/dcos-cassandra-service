package com.mesosphere.dcos.cassandra.common.config;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ExecutorConfig {


    public static Serializer<ExecutorConfig> JSON_SERIALIZER =
            new Serializer<ExecutorConfig>() {
                @Override
                public byte[] serialize(ExecutorConfig value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException(
                                "Error writing ExecutorConfig to JSON",
                                ex);
                    }
                }

                @Override
                public ExecutorConfig deserialize(byte[] bytes)
                        throws SerializationException {

                    try {
                        return JsonUtils.MAPPER.readValue(bytes,
                                ExecutorConfig.class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "ExecutorConfig form JSON", ex);
                    }
                }
            };

    public static ExecutorConfig create(
            String command,
            List<String> arguments,
            double cpus,
            int memoryMb,
            int diskMb,
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
                diskMb,
                heapMb,
                apiPort,
                javaHome,
                jreLocation,
                executorLocation,
                cassandraLocation
        );
    }

    @JsonCreator
    public static ExecutorConfig create(
            @JsonProperty("command") String command,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("disk_mb") int diskMb,
            @JsonProperty("heap_mb") int heapMb,
            @JsonProperty("api_port") int apiPort,
            @JsonProperty("java_home") String javaHome,
            @JsonProperty("jre_location") String jreLocation,
            @JsonProperty("executor_location") String executorLocation,
            @JsonProperty("cassandra_location") String cassandraLocation)
            throws URISyntaxException, UnsupportedEncodingException {

        ExecutorConfig config = create(
                command,
                arguments,
                cpus,
                memoryMb,
                diskMb,
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
    @JsonProperty("disk_mb")
    private final int diskMb;
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
            int diskMb,
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
        this.diskMb = diskMb;
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

    public int getDiskMb() {
        return diskMb;
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
    public List<URI> getURIs() {
        return Arrays.asList(executorLocation,cassandraLocation,jreLocation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutorConfig)) return false;
        ExecutorConfig that = (ExecutorConfig) o;
        return Double.compare(that.getCpus(), getCpus()) == 0 &&
                getMemoryMb() == that.getMemoryMb() &&
                getDiskMb() == that.getDiskMb() &&
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
                getDiskMb(), getHeapMb(), getApiPort(),
                getJreLocation(), getExecutorLocation(), getCassandraLocation(),
                getJavaHome());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
