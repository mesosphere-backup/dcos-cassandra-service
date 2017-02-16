/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * MetricConfig is the configuration to emit StatsD metrics for the cassandra tasks.
 * It is serializable to both JSON and Protocol Buffers.
 */
public class MetricConfig {

    @JsonCreator
    public static MetricConfig create(
            @JsonProperty("host") String host,
            @JsonProperty("arguments") int port,
            @JsonProperty("cpus") String prefix,
            @JsonProperty("memory_mb") int frequency,
            @JsonProperty("enabled") boolean enabled)
            throws URISyntaxException, UnsupportedEncodingException {

        MetricConfig config = new MetricConfig(
                host,
                port,
                prefix,
                frequency,
                enabled);

        return config;
    }

    @JsonProperty("host")
    private final String host;

    @JsonProperty("port")
    private final int port;

    @JsonProperty("prefix")
    private final String prefix;

    @JsonProperty("frequency")
    private final int frequency;

    @JsonProperty("enabled")
    private final boolean enabled;

    /**
     *
     * @param host
     * @param port
     * @param prefix
     * @param frequency
     */
    public MetricConfig(final String host,
                        final int port,
                        final String prefix,
                        final int frequency,
                        final boolean enabled) {
        this.host = host;
        this.port = port;
        this.prefix = prefix;
        this.frequency = frequency;
        this.enabled = enabled;
    }

    public String getHost() { return host; }

    public int getPort() { return port; }

    public String getPrefix() { return prefix; }

    public int getFrequency() { return frequency; }

    public boolean getEnabled() { return enabled; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricConfig)) return false;
        MetricConfig that = (MetricConfig) o;
        return getPort() == that.getPort() &&
                getFrequency() == that.getFrequency() &&
                Objects.equals(getHost(), that.getHost()) &&
                Objects.equals(getPrefix(), that.getPrefix()) &&
                enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPort(), getFrequency(), getHost(), getPrefix());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
