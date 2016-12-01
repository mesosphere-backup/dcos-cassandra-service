package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.Configuration;
import org.apache.mesos.config.SerializationUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Object representation of the scheduler configuration that's serialized to the config store.
 *
 * Enables the {@code ignoreUnknown} setting to ensure that removed fields do not cause config
 * deserialization to fail. For example, if an old configuration still specifies "placement_strategy",
 * this setting prevents that now-unknown field from breaking the parsing operation.
 *
 * @see JsonIgnoreProperties#ignoreUnknown()
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CassandraSchedulerConfiguration implements Configuration {

  @JsonCreator
  public static CassandraSchedulerConfiguration create(
    @JsonProperty("executor") final ExecutorConfig executorConfig,
    @JsonProperty("servers") final int servers,
    @JsonProperty("seeds") final int seeds,
    @JsonProperty("marathon_placement") final String marathonPlacement,
    @JsonProperty("cassandra") final CassandraConfig cassandraConfig,
    @JsonProperty("cluster_task") final ClusterTaskConfig clusterTaskConfig,
    @JsonProperty("api_port") final int apiPort,
    @JsonProperty("service") final ServiceConfig serviceConfig,
    @JsonProperty("external_dc_sync_ms") final long externalDcSyncMs,
    @JsonProperty("external_dcs") final String externalDcs,
    @JsonProperty("dc_url") final String dcUrl,
    @JsonProperty("phase_strategy") final String phaseStrategy,
    @JsonProperty("enable_upgrade_sstable_endpoint") final boolean enableUpgradeSSTableEndpoint) {

    return new CassandraSchedulerConfiguration(
      executorConfig,
      servers,
      seeds,
      marathonPlacement,
      cassandraConfig,
      clusterTaskConfig,
      apiPort,
      serviceConfig,
      externalDcSyncMs,
      externalDcs,
      dcUrl,
      phaseStrategy,
      enableUpgradeSSTableEndpoint
    );
  }

  @JsonIgnore
  private final ExecutorConfig executorConfig;
  @JsonIgnore
  private final int servers;
  @JsonIgnore
  private final int seeds;
  @JsonIgnore
  private final String marathonPlacement;
  @JsonIgnore
  private final CassandraConfig cassandraConfig;
  @JsonIgnore
  private final ClusterTaskConfig clusterTaskConfig;
  @JsonIgnore
  private final int apiPort;
  @JsonIgnore
  private final ServiceConfig serviceConfig;
  @JsonIgnore
  private final long externalDcSyncMs;
  @JsonIgnore
  private final String externalDcs;
  @JsonIgnore
  private final String dcUrl;
  @JsonIgnore
  private final String phaseStrategy;
  @JsonIgnore
  private final boolean enableUpgradeSSTableEndpoint;

  private CassandraSchedulerConfiguration(
    ExecutorConfig executorConfig,
    int servers,
    int seeds,
    String marathonPlacement,
    CassandraConfig cassandraConfig,
    ClusterTaskConfig clusterTaskConfig,
    int apiPort, ServiceConfig serviceConfig,
    long externalDcSyncMs,
    String externalDcs,
    String dcUrl,
    String phaseStrategy,
    boolean enableUpgradeSSTableEndpoint) {
    this.executorConfig = executorConfig;
    this.servers = servers;
    this.seeds = seeds;
    this.marathonPlacement = marathonPlacement;
    this.cassandraConfig = cassandraConfig;
    this.clusterTaskConfig = clusterTaskConfig;
    this.apiPort = apiPort;
    this.serviceConfig = serviceConfig;
    this.externalDcSyncMs = externalDcSyncMs;
    this.externalDcs = externalDcs;
    this.dcUrl = dcUrl;
    this.phaseStrategy = phaseStrategy;
    this.enableUpgradeSSTableEndpoint = enableUpgradeSSTableEndpoint;
  }

  @JsonProperty("executor")
  public ExecutorConfig getExecutorConfig() {
    return executorConfig;
  }

  @JsonProperty("servers")
  public int getServers() {
    return servers;
  }

  @JsonProperty("seeds")
  public int getSeeds() {
    return seeds;
  }

  @JsonProperty("marathon_placement")
  public String getMarathonPlacement() {
    return marathonPlacement;
  }

  @JsonProperty("cassandra")
  public CassandraConfig getCassandraConfig() {
    return cassandraConfig;
  }

  @JsonProperty("cluster_task")
  public ClusterTaskConfig getClusterTaskConfig() {
    return clusterTaskConfig;
  }

  @JsonProperty("api_port")
  public int getApiPort() {
    return apiPort;
  }

  @JsonProperty("service")
  public ServiceConfig getServiceConfig() {
    return serviceConfig;
  }

  @JsonProperty("external_dc_sync_ms")
  public long getExternalDcSyncMs() {
    return externalDcSyncMs;
  }

  @JsonProperty("external_dcs")
  public String getExternalDcs() {
    return externalDcs;
  }

  @JsonProperty("dc_url")
  public String getDcUrl() {
    return dcUrl;
  }

  @JsonProperty("phase_strategy")
  public String getPhaseStrategy() {
    return phaseStrategy;
  }

  @JsonProperty("enable_upgrade_sstable_endpoint")
  public boolean getEnableUpgradeSSTableEndpoint() { return enableUpgradeSSTableEndpoint; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CassandraSchedulerConfiguration that = (CassandraSchedulerConfiguration) o;
    return servers == that.servers &&
      seeds == that.seeds &&
      apiPort == that.apiPort &&
      externalDcSyncMs == that.externalDcSyncMs &&
      enableUpgradeSSTableEndpoint == that.enableUpgradeSSTableEndpoint &&
      Objects.equals(executorConfig, that.executorConfig) &&
      Objects.equals(marathonPlacement, that.marathonPlacement) &&
      Objects.equals(cassandraConfig, that.cassandraConfig) &&
      Objects.equals(clusterTaskConfig, that.clusterTaskConfig) &&
      Objects.equals(serviceConfig, that.serviceConfig) &&
      Objects.equals(externalDcs, that.externalDcs) &&
      Objects.equals(dcUrl, that.dcUrl) &&
      Objects.equals(phaseStrategy, that.phaseStrategy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      executorConfig,
      servers,
      seeds,
      marathonPlacement,
      cassandraConfig,
      clusterTaskConfig,
      apiPort,
      serviceConfig,
      externalDcSyncMs,
      externalDcs,
      dcUrl,
      phaseStrategy,
      enableUpgradeSSTableEndpoint);
  }

  @JsonIgnore
  public List<String> getExternalDcsList() {
    if (externalDcs == null || externalDcs.isEmpty())
      return Collections.emptyList();
    else {
      return Arrays.asList(externalDcs.split(","))
        .stream()
        .filter(dc -> !dc.isEmpty())
        .collect(Collectors.toList());
    }
  }

  @JsonIgnore
  @Override
  public byte[] getBytes() throws ConfigStoreException {
    try {
      return SerializationUtils.toJsonString(this).getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      e.printStackTrace();
      throw new ConfigStoreException(e);
    }
  }

  @JsonIgnore
  @Override
  public String toJsonString() throws ConfigStoreException {
    return JsonUtils.toJsonString(this);
  }
}
