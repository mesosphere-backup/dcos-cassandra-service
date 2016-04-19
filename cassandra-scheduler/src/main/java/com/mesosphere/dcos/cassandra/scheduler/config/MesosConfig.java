package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.time.Duration;

public class MesosConfig {

    @JsonProperty("servers")
    private final String servers;
    @JsonProperty("path")
    private final String path;
    private final Duration timeout;

    public static MesosConfig create(String servers,
                                     String path,
                                     Duration timeout) {

        return new MesosConfig(servers, path, timeout);
    }

    @JsonCreator
    public static MesosConfig create(@JsonProperty("servers") String servers,
                                     @JsonProperty("path") String path,
                                     @JsonProperty("timeout_ms") Long
                                                 timeoutMs) {

        return create(servers,
                path,
                Duration.ofMillis(timeoutMs));
    }

    public MesosConfig(String servers, String path, Duration timeout) {
        this.servers = servers;
        this.path = path;
        this.timeout = timeout;
    }

    public String getServers() {
        return servers;
    }

    public String getPath() {
        return path;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public String toZooKeeperUrl() {
        return "zk://" + servers + path;
    }

    @JsonProperty("timeout_ms")
    public Long getTimeoutMillis() {

        return timeout.toMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MesosConfig)) return false;

        MesosConfig that = (MesosConfig) o;

        if (getServers() != null ? !getServers().equals(
                that.getServers()) : that.getServers() != null) return false;
        if (getPath() != null ? !getPath().equals(
                that.getPath()) : that.getPath() != null) return false;
        return getTimeout() != null ? getTimeout().equals(
                that.getTimeout()) : that.getTimeout() == null;

    }

    @Override
    public int hashCode() {
        int result = getServers() != null ? getServers().hashCode() : 0;
        result = 31 * result + (getPath() != null ? getPath().hashCode() : 0);
        result = 31 * result + (getTimeout() != null ? getTimeout().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }


}
