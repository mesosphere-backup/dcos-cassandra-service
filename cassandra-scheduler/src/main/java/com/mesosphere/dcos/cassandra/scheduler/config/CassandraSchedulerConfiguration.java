package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class CassandraSchedulerConfiguration implements Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSchedulerConfiguration.class);

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

    @Override
    @JsonIgnore
    public byte[] getBytes() throws ConfigStoreException {
        try {
            return toJsonString().getBytes();
        } catch (Exception e) {
            LOGGER.error("Error occured while serializing the object: " + e);
            throw new ConfigStoreException(e);
        }
    }

    @Override
    public String toJsonString() throws Exception {
        return JsonUtils.toJsonString(this);
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
                Objects.equals(name, that.name) &&
                Objects.equals(version, that.version) &&
                Objects.equals(placementStrategy, that.placementStrategy) &&
                Objects.equals(cassandraConfig, that.cassandraConfig) &&
                Objects.equals(clusterTaskConfig, that.clusterTaskConfig) &&
                Objects.equals(identity, that.identity) &&
                Objects.equals(phaseStrategy, that.phaseStrategy) &&
                Objects.equals(mesosConfig, that.mesosConfig) &&
                Objects.equals(curatorConfig, that.curatorConfig) &&
                Objects.equals(seedsUrl, that.seedsUrl) &&
                Objects.equals(externalDcs, that.externalDcs) &&
                Objects.equals(dcUrl, that.dcUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executorConfig, name, version, servers, seeds, placementStrategy, cassandraConfig,
                clusterTaskConfig, apiPort, identity, phaseStrategy, mesosConfig, curatorConfig, seedsUrl,
                externalDcSyncMs, externalDcs, dcUrl);
    }
}
