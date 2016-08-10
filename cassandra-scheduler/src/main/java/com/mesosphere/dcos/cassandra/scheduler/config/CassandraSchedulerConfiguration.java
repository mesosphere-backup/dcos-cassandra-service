package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.Configuration;

import java.util.*;
import java.util.stream.Collectors;

public class CassandraSchedulerConfiguration implements Configuration {

  @JsonCreator
  public static CassandraSchedulerConfiguration create(
    @JsonProperty("executor") final ExecutorConfig executorConfig,
    @JsonProperty("servers") final int servers,
    @JsonProperty("seeds") final int seeds,
    @JsonProperty("placement_strategy") final String placementStrategy,
    @JsonProperty("cassandra") final CassandraConfig cassandraConfig,
    @JsonProperty("cluster_task") final ClusterTaskConfig clusterTaskConfig,
    @JsonProperty("api_port") final int apiPort,
    @JsonProperty("service") final ServiceConfig serviceConfig,
    @JsonProperty("external_dc_sync_ms") final long externalDcSyncMs,
    @JsonProperty("external_dcs") final String externalDcs,
    @JsonProperty("dc_url") final String dcUrl,
    @JsonProperty("phase_strategy") final String phaseStrategy) {

    return new CassandraSchedulerConfiguration(
      executorConfig,
      servers,
      seeds,
      placementStrategy,
      cassandraConfig,
      clusterTaskConfig,
      apiPort,
      serviceConfig,
      externalDcSyncMs,
      externalDcs,
      dcUrl,
      phaseStrategy
    );
  }

  @JsonIgnore
  private final ExecutorConfig executorConfig;
  @JsonIgnore
  private final int servers;
  @JsonIgnore
  private final int seeds;
  @JsonIgnore
  private final String placementStrategy;
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

  private CassandraSchedulerConfiguration(
    ExecutorConfig executorConfig,
    int servers,
    int seeds,
    String placementStrategy,
    CassandraConfig cassandraConfig,
    ClusterTaskConfig clusterTaskConfig,
    int apiPort, ServiceConfig serviceConfig,
    long externalDcSyncMs,
    String externalDcs,
    String dcUrl,
    String phaseStrategy) {
    this.executorConfig = executorConfig;
    this.servers = servers;
    this.seeds = seeds;
    this.placementStrategy = placementStrategy;
    this.cassandraConfig = cassandraConfig;
    this.clusterTaskConfig = clusterTaskConfig;
    this.apiPort = apiPort;
    this.serviceConfig = serviceConfig;
    this.externalDcSyncMs = externalDcSyncMs;
    this.externalDcs = externalDcs;
    this.dcUrl = dcUrl;
    this.phaseStrategy = phaseStrategy;
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

  @JsonProperty("placement_strategy")
  public String getPlacementStrategy() {
    return placementStrategy;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CassandraSchedulerConfiguration that = (CassandraSchedulerConfiguration) o;
    return servers == that.servers &&
      seeds == that.seeds &&
      apiPort == that.apiPort &&
      externalDcSyncMs == that.externalDcSyncMs &&
      Objects.equals(executorConfig, that.executorConfig) &&
      Objects.equals(placementStrategy, that.placementStrategy) &&
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
      placementStrategy,
      cassandraConfig,
      clusterTaskConfig,
      apiPort,
      serviceConfig,
      externalDcSyncMs,
      externalDcs,
      dcUrl,
      phaseStrategy);
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
      return JsonUtils.MAPPER.writeValueAsBytes(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new ConfigStoreException(e);
    }
  }

  @JsonIgnore
  @Override
  public String toJsonString() throws Exception {
    return JsonUtils.toJsonString(this);
  }
}
