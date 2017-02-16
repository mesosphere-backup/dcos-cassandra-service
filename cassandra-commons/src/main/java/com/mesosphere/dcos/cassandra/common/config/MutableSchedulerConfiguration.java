package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;

import java.util.Objects;
import java.util.Optional;

public class MutableSchedulerConfiguration extends Configuration {

  private ExecutorConfig executorConfig;
  private int servers;
  private int seeds;
  private String placementConstraint;
  private CassandraConfig cassandraConfig;
  private ClusterTaskConfig clusterTaskConfig;
  private int apiPort;
  private ServiceConfig serviceConfig;
  private MesosConfig mesosConfig =
    MesosConfig.create(
      "master.mesos:2181",
      "/mesos",
      10000L,
      5
    );
  private CuratorFrameworkConfig curatorConfig =
    CuratorFrameworkConfig.create(
      "master.mesos:2181",
      10000L,
      10000L,
      Optional.empty(),
      250L,
      "",
      "");
  private long externalDcSyncMs;
  private String externalDcs;
  private String dcUrl;
  private String phaseStrategy;
  private boolean enableUpgradeSSTableEndpoint;
  private HttpClientConfiguration httpClientConfiguration;
  private MetricConfig metricConfig;

  @JsonProperty("mesos")
  public MesosConfig getMesosConfig() {
    return mesosConfig;
  }

  @JsonProperty("zookeeper")
  public CuratorFrameworkConfig getCuratorConfig() {
    return curatorConfig;
  }

  @JsonProperty("cassandra")
  public CassandraConfig getCassandraConfig() {
    return cassandraConfig;
  }


  @JsonProperty("mesos")
  public void setMesosConfig(MesosConfig mesosConfig) {
    this.mesosConfig = mesosConfig;
  }

  @JsonProperty("zookeeper")
  public void setCuratorConfig(
    CuratorFrameworkConfig curatorConfig) {
    this.curatorConfig = curatorConfig;

  }

  @JsonProperty("cassandra")
  public void setCassandraConfig(
    CassandraConfig cassandraConfig) {
    this.cassandraConfig = cassandraConfig;
  }

  @JsonProperty("executor")
  public ExecutorConfig getExecutorConfig() {
    return executorConfig;
  }

  @JsonProperty("executor")
  public void setExecutorConfig(
    ExecutorConfig executorConfig) {
    this.executorConfig = executorConfig;
  }

  @JsonProperty("cluster_task")
  public ClusterTaskConfig getClusterTaskConfig() {
    return clusterTaskConfig;
  }

  @JsonProperty("cluster_task")
  public void setClusterTaskConfig(
    ClusterTaskConfig clusterTaskConfig) {
    this.clusterTaskConfig = clusterTaskConfig;
  }

  @JsonProperty("seed_nodes")
  public int getSeeds() {
    return seeds;
  }

  @JsonProperty("seed_nodes")
  public void setSeeds(int seeds) {
    this.seeds = seeds;
  }

  @JsonProperty("nodes")
  public int getServers() {
    return servers;
  }

  @JsonProperty("nodes")
  public void setServers(int servers) {
    this.servers = servers;
  }

  @JsonProperty("placement_constraint")
  public String getPlacementConstraint() {
    return placementConstraint;
  }

  @JsonProperty("placement_constraint")
  public void setPlacementConstraint(String placementConstraint) {
    this.placementConstraint = placementConstraint;
  }

  @JsonProperty("phase_strategy")
  public String getPhaseStrategy() {
    return phaseStrategy;
  }

  @JsonProperty("phase_strategy")
  void setPhaseStrategy(String phaseStrategy){
    this.phaseStrategy = phaseStrategy;
  }

  @JsonProperty("api_port")
  public int getApiPort() {
    return apiPort;
  }

  @JsonProperty("api_port")
  public void setApiPort(int port) {
    this.apiPort = port;
  }

  @JsonProperty("service")
  public ServiceConfig getServiceConfig() {
    return serviceConfig;
  }


  @JsonProperty("service")
  public void setServiceConfig(ServiceConfig serviceConfig) {
    this.serviceConfig = serviceConfig;
  }

  @JsonProperty("dc_sync_ms")
  public long getExternalDcSyncMs() {
    return externalDcSyncMs;
  }

  @JsonProperty("dc_sync_ms")
  public void setExternalDcSyncMs(long externalDcSyncMs) {
    this.externalDcSyncMs = externalDcSyncMs;
  }

  @JsonProperty("dc_url")
  public String getDcUrl() {
    return dcUrl;
  }

  @JsonProperty("dc_url")
  public void setDcUrl(String dcUrl) {
    this.dcUrl = dcUrl;
  }

  @JsonProperty("external_dcs")
  public String getExternalDcs() {
    return externalDcs;
  }

  @JsonProperty("external_dcs")
  public void setExternalDcs(String externalDcs) {
    this.externalDcs = externalDcs;
  }

  @JsonProperty("enable_upgrade_sstable_endpoint")
  public boolean getEnableUpgradeSStableEndpoint() { return enableUpgradeSSTableEndpoint; }

  @JsonProperty("enable_upgrade_sstable_endpoint")
  public void setEnableUpgradeSSTableEndpoint(boolean enableUpgradeSSTableEndpoint) {
    this.enableUpgradeSSTableEndpoint = enableUpgradeSSTableEndpoint;
  }

  @JsonProperty("http_client")
  public HttpClientConfiguration getHttpClientConfiguration() { return httpClientConfiguration; }

  @JsonProperty("http_client")
  public void setHttpClientConfiguration(HttpClientConfiguration httpClientConfiguration) {
    this.httpClientConfiguration = httpClientConfiguration;
  }

  @JsonProperty("metrics")
  public MetricConfig getMetricConfig() { return metricConfig; }

  @JsonProperty("metrics")
  public void setMetricConfig(MetricConfig metricConfig) {
    this.metricConfig = metricConfig;
  }

  @JsonIgnore
  public CassandraSchedulerConfiguration createConfig() {
    return CassandraSchedulerConfiguration.create(
      executorConfig,
      servers,
      seeds,
      placementConstraint,
      cassandraConfig,
      clusterTaskConfig,
      apiPort,
      serviceConfig,
      externalDcSyncMs,
      externalDcs,
      dcUrl,
      phaseStrategy,
      enableUpgradeSSTableEndpoint,
      httpClientConfiguration,
      metricConfig
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MutableSchedulerConfiguration that = (MutableSchedulerConfiguration) o;
    return servers == that.servers &&
      seeds == that.seeds &&
      apiPort == that.apiPort &&
      externalDcSyncMs == that.externalDcSyncMs &&
      enableUpgradeSSTableEndpoint == that.enableUpgradeSSTableEndpoint &&
      Objects.equals(executorConfig, that.executorConfig) &&
      Objects.equals(placementConstraint, that.placementConstraint) &&
      Objects.equals(cassandraConfig, that.cassandraConfig) &&
      Objects.equals(clusterTaskConfig, that.clusterTaskConfig) &&
      Objects.equals(serviceConfig, that.serviceConfig) &&
      Objects.equals(mesosConfig, that.mesosConfig) &&
      Objects.equals(curatorConfig, that.curatorConfig) &&
      Objects.equals(externalDcs, that.externalDcs) &&
      Objects.equals(dcUrl, that.dcUrl) &&
      Objects.equals(httpClientConfiguration, that.httpClientConfiguration) &&
      Objects.equals(metricConfig, that.mesosConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executorConfig, servers, seeds, placementConstraint, cassandraConfig,
      clusterTaskConfig, apiPort, serviceConfig, mesosConfig, curatorConfig,
      externalDcSyncMs, externalDcs, dcUrl, enableUpgradeSSTableEndpoint, httpClientConfiguration, metricConfig);
  }

  @Override
  public String toString() {
    return JsonUtils.toJsonString(this);
  }
}
