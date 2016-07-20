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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    @JsonProperty("keytab_location") String keytabLocation,
    @JsonProperty("cassandra_ulimit_memlock") String cassandraUlimitMemlock,
    @JsonProperty("cassandra_ulimit_nofile") String cassandraUlimitNofile,
    @JsonProperty("cassandra_ulimit_nproc") String cassandraUlimitNproc) {
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
      cassandraLocation,
      keytabLocation,
      cassandraUlimitMemlock,
      cassandraUlimitNofile,
      cassandraUlimitNproc);
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
  @JsonProperty("jre_location")
  private final String jreLocation;
  @JsonProperty("executor_location")
  private final String executorLocation;
  @JsonProperty("cassandra_location")
  private final String cassandraLocation;
  @JsonProperty("keytab_location")
  private final String keytabLocation;
  @JsonProperty("cassandra_ulimit_memlock")
  private final String cassandraUlimitMemlock;
  @JsonProperty("cassandra_ulimit_nofile")
  private final String cassandraUlimitNofile;
  @JsonProperty("cassandra_ulimit_nproc")
  private final String cassandraUlimitNproc;

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
    String jreLocation,
    String executorLocation,
    String cassandraLocation,
    String keytabLocation,
    String cassandraUlimitMemlock,
    String cassandraUlimitNofile,
    String cassandraUlimitNproc) {

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
    this.cassandraUlimitMemlock = cassandraUlimitMemlock;
    this.cassandraUlimitNofile = cassandraUlimitNofile;
    this.cassandraUlimitNproc = cassandraUlimitNproc;
    this.keytabLocation = keytabLocation;
  }


  public int getApiPort() {
    return apiPort;
  }

  public List<String> getArguments() {
    return arguments;
  }

  public String getCassandraLocation() {
    return cassandraLocation;
  }

  public String getCommand() {
    return command;
  }

  public double getCpus() {
    return cpus;
  }

  public String getExecutorLocation() {
    return executorLocation;
  }

  public int getHeapMb() {
    return heapMb;
  }

  public String getJavaHome() {
    return javaHome;
  }

  public String getJreLocation() {
    return jreLocation;
  }

  public int getMemoryMb() {
    return memoryMb;
  }

  public String getKeytabLocation() {
    return keytabLocation;
  }

  @JsonProperty("cassandra_ulimit_memlock")
  public String getCassandraUlimitMemlock() {
    return cassandraUlimitMemlock;
  }

  @JsonProperty("cassandra_ulimit_nofile")
  public String getCassandraUlimitNofile() {
    return cassandraUlimitNofile;
  }

  @JsonProperty("cassandra_ulimit_nproc")
  public String getCassandraUlimitNproc() {
    return cassandraUlimitNproc;
  }


  @JsonIgnore
  public Set<String> getURIs() {
    Set<String> uris = new HashSet<String>();
    uris.add(executorLocation.toString());
    uris.add(cassandraLocation.toString());
    uris.add(jreLocation.toString());
    if (keytabLocation != null && !keytabLocation.isEmpty()) {
      uris.add(keytabLocation);
    }

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
      Objects.equals(getJavaHome(), that.getJavaHome()) &&
      Objects.equals(getKeytabLocation(), that.getKeytabLocation());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCommand(), getArguments(), getCpus(),
      getMemoryMb(),
      getHeapMb(), getApiPort(),
      getJreLocation(), getExecutorLocation(), getCassandraLocation(),
      getJavaHome(),
      getKeytabLocation());
  }

  @Override
  public String toString() {
    return JsonUtils.toJsonString(this);
  }
>>>>>>> update DSE configuration builder
}
