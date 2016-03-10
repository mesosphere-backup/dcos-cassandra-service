package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

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
    private String planStrategy;
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

    @JsonProperty("frameworkVersion")
    public String getVersion() {
        return version;
    }

    @JsonProperty("frameworkVersion")
    public CassandraSchedulerConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }

    @JsonProperty("frameworkName")
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

    @JsonProperty("frameworkName")
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

    @JsonProperty("clusterTask")
    public ClusterTaskConfig getClusterTaskConfig() {
        return clusterTaskConfig;
    }

    @JsonProperty("clusterTask")
    public CassandraSchedulerConfiguration setClusterTaskConfig(ClusterTaskConfig clusterTaskConfig) {
        this.clusterTaskConfig = clusterTaskConfig;
        return this;
    }

    @JsonProperty("seedNodes")
    public int getSeeds() {
        return seeds;
    }

    @JsonProperty("seedNodes")
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

    @JsonProperty("placementStrategy")
    public String getPlacementStrategy() {
        return placementStrategy;
    }

    @JsonProperty("placementStrategy")
    public CassandraSchedulerConfiguration setPlacementStrategy(String placementStrategy) {
        this.placementStrategy = placementStrategy;
        return this;
    }


    @JsonProperty("apiPort")
    public int getApiPort() {
        return apiPort;
    }

    @JsonProperty("apiPort")
    public CassandraSchedulerConfiguration setApiPort(int port) {
        this.apiPort = port;
        return this;
    }

    @JsonProperty("seedsUrl")
    public String getSeedsUrl() {
        return seedsUrl;
    }

    @JsonProperty("seedsUrl")
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

    @JsonProperty("phaseStrategy")
    public String getPhaseStrategy() {
        return phaseStrategy;
    }

    @JsonProperty("phaseStrategy")
    public CassandraSchedulerConfiguration setPhaseStrategy(
            String phaseStrategy) {
        this.phaseStrategy = phaseStrategy;
        return this;
    }

    @JsonIgnore
    public CassandraConfig getCassandraConfig() {
        return cassandraConfig.getCassandraConfig(name, getSeedsUrl());
    }


}
