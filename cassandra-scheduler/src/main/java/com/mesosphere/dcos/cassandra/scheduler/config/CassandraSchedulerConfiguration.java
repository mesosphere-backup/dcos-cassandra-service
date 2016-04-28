package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CassandraSchedulerConfiguration extends Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraSchedulerConfiguration.class
    );

    private ExecutorConfig executorConfig;
    private String name;
    private String version;
    private int servers;
    private int seeds;
    private String placementStrategy;
    private CassandraConfigParser cassandraConfig;
    private ClusterTaskConfig clusterTaskConfig;
    private int apiPort;
    private Identity identity;
    private String phaseStrategy;
    private MesosConfig mesosConfig =
            MesosConfig.create(
                    "master.mesos:2181",
                    "/mesos",
                    10000L
            );
    private CuratorFrameworkConfig curatorConfig =
            CuratorFrameworkConfig.create(
                    "master.mesos:2181",
                    10000L,
                    10000L,
                    Optional.empty(),
                    250L);
    private String seedsUrl;
    private long externalDcSyncMs;
    private String externalDcs;
    private String dcUrl;

    @JsonProperty("framework_version")
    public String getVersion() {
        return version;
    }

    @JsonProperty("framework_version")
    public CassandraSchedulerConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }

    @JsonProperty("framework_name")
    public String getName() {
        return name;
    }

    @JsonProperty("mesos")
    public MesosConfig getMesosConfig() {
        return mesosConfig;
    }

    @JsonProperty("zookeeper")
    public CuratorFrameworkConfig getCuratorConfig() {
        return curatorConfig;
    }

    @JsonProperty("cassandra")
    public CassandraConfigParser getCassandraConfigParser() {
        return cassandraConfig;
    }

    @JsonProperty("framework_name")
    public CassandraSchedulerConfiguration setName(String name) {
        this.name = name;
        return this;
    }

    private HttpClientConfiguration httpClient = new HttpClientConfiguration();

    @JsonProperty("httpClient")
    public HttpClientConfiguration getHttpClientConfiguration() {
        return httpClient;
    }

    @JsonProperty("httpClient")
    public void setHttpClientConfiguration(HttpClientConfiguration httpClient) {
        this.httpClient = httpClient;
    }

    @JsonProperty("mesos")
    public CassandraSchedulerConfiguration setMesosConfig(MesosConfig mesosConfig) {
        this.mesosConfig = mesosConfig;
        return this;
    }

    @JsonProperty("zookeeper")
    public CassandraSchedulerConfiguration setCuratorConfig(
            CuratorFrameworkConfig curatorConfig) {
        this.curatorConfig = curatorConfig;
        return this;
    }

    @JsonProperty("cassandra")
    public CassandraSchedulerConfiguration setCassandraConfigParser
            (CassandraConfigParser
                     cassandraConfig) {
        this.cassandraConfig = cassandraConfig;
        return this;
    }

    @JsonProperty("executor")
    public ExecutorConfig getExecutorConfig() {
        return executorConfig;
    }

    @JsonProperty("executor")
    public CassandraSchedulerConfiguration setExecutorConfig(ExecutorConfig executorConfig) {
        this.executorConfig = executorConfig;
        return this;
    }

    @JsonProperty("cluster_task")
    public ClusterTaskConfig getClusterTaskConfig() {
        return clusterTaskConfig;
    }

    @JsonProperty("cluster_task")
    public CassandraSchedulerConfiguration setClusterTaskConfig(
            ClusterTaskConfig clusterTaskConfig) {
        this.clusterTaskConfig = clusterTaskConfig;
        return this;
    }

    @JsonProperty("seed_nodes")
    public int getSeeds() {
        return seeds;
    }

    @JsonProperty("seed_nodes")
    public CassandraSchedulerConfiguration setSeeds(int seeds) {
        this.seeds = seeds;
        return this;
    }

    @JsonProperty("nodes")
    public int getServers() {
        return servers;
    }

    @JsonProperty("nodes")
    public CassandraSchedulerConfiguration setServers(int servers) {
        this.servers = servers;
        return this;
    }

    @JsonProperty("placement_strategy")
    public String getPlacementStrategy() {
        return placementStrategy;
    }

    @JsonProperty("placement_strategy")
    public CassandraSchedulerConfiguration setPlacementStrategy(String placementStrategy) {
        this.placementStrategy = placementStrategy;
        return this;
    }


    @JsonProperty("api_port")
    public int getApiPort() {
        return apiPort;
    }

    @JsonProperty("api_port")
    public CassandraSchedulerConfiguration setApiPort(int port) {
        this.apiPort = port;
        return this;
    }

    @JsonProperty("seeds_url")
    public String getSeedsUrl() {
        return seedsUrl;
    }

    @JsonProperty("seeds_url")
    public CassandraSchedulerConfiguration setSeedsUrl(String seedsUrl) {
        this.seedsUrl = seedsUrl;
        return this;
    }

    @JsonProperty("identity")
    public Identity getIdentity() {
        return identity;
    }


    @JsonProperty("identity")
    public CassandraSchedulerConfiguration setIdentity(Identity identity) {
        this.identity = identity;
        return this;
    }

    @JsonProperty("phase_strategy")
    public String getPhaseStrategy() {
        return phaseStrategy;
    }

    @JsonProperty("phase_strategy")
    public CassandraSchedulerConfiguration setPhaseStrategy(
            String phaseStrategy) {
        this.phaseStrategy = phaseStrategy;
        return this;
    }

    @JsonProperty("dc_sync_ms")
    public long getExternalDcSyncMs() {
        return externalDcSyncMs;
    }

    @JsonProperty("dc_sync_ms")
    public CassandraSchedulerConfiguration setExternalDcSyncMs(long externalDcSyncMs) {
        this.externalDcSyncMs = externalDcSyncMs;
        return this;
    }

    @JsonProperty("dc_url")
    public String getDcUrl() {
        return dcUrl;
    }

    @JsonProperty("dc_url")
    public CassandraSchedulerConfiguration setDcUrl(String dcUrl) {
        this.dcUrl = dcUrl;
        return this;
    }

    @JsonProperty("external_dcs")
    public String getExternalDcs() {
        return externalDcs;
    }

    @JsonProperty("external_dcs")
    public CassandraSchedulerConfiguration setExternalDcs(String externalDcs) {
        this.externalDcs = externalDcs;
        return this;
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
    public CassandraConfig getCassandraConfig() {
        return cassandraConfig.getCassandraConfig(identity.getCluster(),
                getSeedsUrl());
    }


}
