package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.time.Duration;
import java.util.Objects;
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
    @JsonIgnore
    private final String zkusername;
    @JsonIgnore
    private final String zkpassword;

    @JsonCreator
    public static CuratorFrameworkConfig create(
            @JsonProperty("servers") String servers,
            @JsonProperty("session_timeout_ms") Long sessionTimeoutMs,
            @JsonProperty("connection_timeout_ms") Long connectionTimeoutMs,
            @JsonProperty("operation_timeout_ms")
            Optional<Long> operationTimeoutMs,
            @JsonProperty("backoff_ms") Long backoffMs,
            @JsonProperty("zkusername") String zkusername,
            @JsonProperty("zkpassword") String zkpassword) {

        return new CuratorFrameworkConfig(
                servers,
                Duration.ofMillis(sessionTimeoutMs),
                Duration.ofMillis(connectionTimeoutMs),
                operationTimeoutMs.map(Duration::ofMillis),
                Duration.ofMillis(backoffMs),
                zkusername,
                zkpassword);

    }

    public static CuratorFrameworkConfig create(String servers,
                                                Duration sessionTimeout,
                                                Duration connectionTimeout,
                                                Optional<Duration> operationTimeout,
                                                Duration backoff,
                                                String zkusername,
                                                String zkpassword) {
        return new CuratorFrameworkConfig(
                servers,
                sessionTimeout,
                connectionTimeout,
                operationTimeout,
                backoff,
                zkusername,
                zkpassword);
    }

    public CuratorFrameworkConfig(String servers,
                                  Duration sessionTimeout,
                                  Duration connectionTimeout,
                                  Optional<Duration> operationTimeout,
                                  Duration backoff,
                                  String zkusername,
                                  String zkpassword) {
        this.servers = servers;
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
        this.operationTimeout = operationTimeout;
        this.backoff = backoff;
        this.zkusername = zkusername;
        this.zkpassword = zkpassword;
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

    public String getZkusername() { return zkusername; }

    public String getZkpassword() { return zkpassword; }

    @JsonProperty("session_timeout_ms")
    public long getSessionTimeoutMs() {
        return sessionTimeout.toMillis();
    }

    @JsonProperty("connection_timeout_ms")
    public long getConnectionTimeoutMs() {
        return connectionTimeout.toMillis();
    }

    @JsonProperty("operation_timeout_ms")
    public Optional<Long> getOperationTimeoutMs() {
        return operationTimeout.map(timeout -> timeout.toMillis());
    }

    @JsonProperty("backoff_ms")
    public long getBackoffMs() {
        return backoff.toMillis();
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CuratorFrameworkConfig that = (CuratorFrameworkConfig) o;
        return Objects.equals(servers, that.servers) &&
                Objects.equals(sessionTimeout, that.sessionTimeout) &&
                Objects.equals(connectionTimeout, that.connectionTimeout) &&
                Objects.equals(operationTimeout, that.operationTimeout) &&
                Objects.equals(backoff, that.backoff) &&
                Objects.equals(zkusername, that.zkusername) &&
                Objects.equals(zkpassword, that.zkpassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                servers,
                sessionTimeout,
                connectionTimeout,
                operationTimeout,
                backoff,
                zkusername,
                zkpassword);
    }
}
