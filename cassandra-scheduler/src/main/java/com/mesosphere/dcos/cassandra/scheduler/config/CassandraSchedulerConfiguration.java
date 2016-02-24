package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
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
    private boolean updateConfig;
    private String placementStrategy;
    private String planStrategy;
    private CassandraConfigParser cassandraConfig;
    private int apiPort;
    private Identity identity;
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

    @JsonProperty("updateConfig")
    public boolean updateConfig() {
        return updateConfig;
    }

    @JsonProperty("updateConfig")
    public CassandraSchedulerConfiguration updateConfig(
            boolean updateConfig) {
        this.updateConfig = updateConfig;
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

    @JsonProperty("planStrategy")
    public String getPlanStrategy() {
        return planStrategy;
    }

    @JsonProperty("planStrategy")
    public CassandraSchedulerConfiguration setPlanStrategy(String planStrategy) {
        this.planStrategy = planStrategy;
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
    public String getSeedsUrl() { return seedsUrl == null ? getDefaultSeedsUrl() : seedsUrl; }

    @JsonProperty("seedsUrl")
    public CassandraSchedulerConfiguration setSeedsUrl(String seedsUrl) {
        this.seedsUrl = seedsUrl;
        return this;
    }

    @JsonIgnore
    private String getDefaultSeedsUrl() {
        return "http://" + name + ".marathon.mesos:" + apiPort + "/v1/seeds";
    }

    @JsonIgnore
    private Identity getDefaultIdentity() {
        return Identity.create(
            name,
            Optional.empty(),
            version,
            "root",
            name + "_cluster",
            name + "_role",
            name + "_principal",
            Long.valueOf(60 * 60 * 24 * 7),
            Optional.empty(),
            true);
    }

    @JsonProperty("identity")
    public Identity getIdentity() {
        return identity == null ? getDefaultIdentity() : identity;
    }

    @JsonProperty("identity")
    public CassandraSchedulerConfiguration setIdentity(Identity identity) {
        this.identity = identity;
        return this;
    }


    @JsonIgnore
    public CassandraConfig getCassandraConfig(){

        return cassandraConfig.getCassandraConfig(name,getSeedsUrl());
    }
}
