package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.time.Duration;
import java.util.Optional;

public class CuratorFrameworkConfig {

    @JsonProperty("servers")
    private final String servers;
    @JsonIgnore
    private final Duration sessionTimeout;
    @JsonIgnore
    private final Duration connectionTimeout;
    @JsonIgnore
    private final Optional<Duration> operationTimeout;
    @JsonIgnore
    private final Duration backoff;

    @JsonCreator
    public static CuratorFrameworkConfig create(
            @JsonProperty("servers") String servers,
            @JsonProperty("sessionTimeoutMs") Long sessionTimeoutMs,
            @JsonProperty("connectionTimeoutMs") Long connectionTimeoutMs,
            @JsonProperty("operationTimeoutMs")
            Optional<Long> operationTimeoutMs,
            @JsonProperty("backoffMs") Long backoffMs) {

        return new CuratorFrameworkConfig(
                servers,
                Duration.ofMillis(sessionTimeoutMs),
                Duration.ofMillis(connectionTimeoutMs),
                operationTimeoutMs.map(Duration::ofMillis),
                Duration.ofMillis(backoffMs));

    }

    public static CuratorFrameworkConfig create(String servers,
                                                Duration sessionTimeout,
                                                Duration connectionTimeout,
                                                Optional<Duration> operationTimeout,
                                                Duration backoff) {
        return new CuratorFrameworkConfig(
                servers,
                sessionTimeout,
                connectionTimeout,
                operationTimeout,
                backoff);
    }

    public CuratorFrameworkConfig(String servers,
                                  Duration sessionTimeout,
                                  Duration connectionTimeout,
                                  Optional<Duration> operationTimeout,
                                  Duration backoff) {
        this.servers = servers;
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
        this.operationTimeout = operationTimeout;
        this.backoff = backoff;
    }

    public String getServers() {
        return servers;
    }

    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Optional<Duration> getOperationTimeout() {
        return operationTimeout;
    }

    public Duration getBackoff() {
        return backoff;
    }

    @JsonProperty("sessionTimeoutMs")
    public long getSessionTimeoutMs() {
        return sessionTimeout.toMillis();
    }

    @JsonProperty("connectionTimeoutMs")
    public long getConnectionTimeoutMs() {
        return connectionTimeout.toMillis();
    }

    @JsonProperty("operationTimeoutMs")
    public Optional<Long> getOperationTimeoutMs() {
        return operationTimeout.map(timeout -> timeout.toMillis());
    }

    @JsonProperty("backoffMs")
    public long getBackoffMs() {
        return backoff.toMillis();
    }

    @Override
    public String toString(){
        return JsonUtils.toJsonString(this);
    }
}
