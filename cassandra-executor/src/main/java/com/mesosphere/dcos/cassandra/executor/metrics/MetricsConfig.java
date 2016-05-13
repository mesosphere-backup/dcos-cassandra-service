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
package com.mesosphere.dcos.cassandra.executor.metrics;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MetricsConfig implements static utility methods to support the
 * configuration of DCOS metrics collection for Cassandra.
 */
public class MetricsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger
            (MetricsConfig.class);

    private static final String CONFIG_FILE = "metrics-reporter-config.yaml";
    private static final String ENV_VALUE =
            "-Dcassandra.metricsReporterConfigFile=" + CONFIG_FILE;
    private static final String ENV_KEY = "JVM_EXTRA_OPTS";
    private static final String STATSD_HOST_ENV = "STATSD_UDP_HOST";
    private static final String STATSD_PORT_ENV = "STATSD_UDP_PORT";
    private final CassandraTaskExecutor executor;

    public MetricsConfig(CassandraTaskExecutor executor) {
        this.executor = executor;
    }

    /***
     * Tests if metrics collection is enabled.
     * @return True if metrics collection is enabled.
     */
    public boolean metricsEnabled() {
        return executor.isMetricsEnable() ||
                (System.getenv(STATSD_HOST_ENV) != null &&
                        System.getenv(STATSD_PORT_ENV) != null);
    }

    private String getMetricsHost() {
        String host = System.getenv(STATSD_HOST_ENV);
        if (host == null) {
            host = executor.getMetricsHost();
        }
        return host;
    }

    private int getMetricsPort() {
        int port = -1;
        String portStr = System.getenv(STATSD_PORT_ENV);
        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException ne) {
                LOGGER.error("Failed to parse port parameter", ne);
            }
        } else {
            port = executor.getMetricsPort();
        }
        return port;
    }

    private String getMetricsPrefix() {
        String prefix = executor.getMetricsPrefix();
        if (executor.getMetricsPrefixIncludeHostname()) {
            try {
                prefix = prefix + "." + InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                prefix = prefix + ".unknown-host";
            }
        }
        return prefix;
    }

    /**
     * Writes the metrics configuration file.
     * @param dir The directory where the configuration file will be written.
     * @return True if the metrics configuration file was written.
     */
    public boolean writeMetricsConfig(final Path dir) {
        if (!metricsEnabled()) {
            LOGGER.info("Metrics is not enabled");
            return false;
        }

        LOGGER.info("Building {}", CONFIG_FILE);
        Map<String, Object> hostMap = new HashMap<>();
        hostMap.put("host", getMetricsHost());

        int port = getMetricsPort();
        if (port == -1) {
            return false;
        }
        hostMap.put("port", port);

        List<Object> hostMapList = new ArrayList<>();
        hostMapList.add(hostMap);

        Map<String, Object> statsdMap = new HashMap<>();
        statsdMap.put("period", executor.getMetricsFlushPeriod());
        statsdMap.put("timeunit", executor.getMetricsFlushPeriodUnit());
        statsdMap.put("hosts", hostMapList);
        statsdMap.put("prefix", getMetricsPrefix());

        List<Object> statsdMapList = new ArrayList<>();
        statsdMapList.add(statsdMap);
        final Map<String, Object> yamlMap = new HashMap<>();
        yamlMap.put(executor.getMetricsCollector(), statsdMapList);
        LOGGER.info("Writing {}", CONFIG_FILE);
        final Yaml yaml = new Yaml();
        final File metricsYaml = dir.resolve(CONFIG_FILE).toFile();
        try (BufferedWriter bw = new BufferedWriter(
                new FileWriter(metricsYaml))) {
            yaml.dump(yamlMap, bw);
        } catch (IOException ex) {
            LOGGER.error("Failed to write configuration", ex);
            return false;
        }
        LOGGER.info("Wrote {}", CONFIG_FILE);
        return true;
    }

    /**
     * Sets the environment variables that will trigger the Cassandra process
     * to output metrics to the DCOS collector.
     * @param env The system environment variables map that will be updated
     *            to contain the metrics collection configuration parameters.
     */
    public void setEnv(final Map<String, String> env) {
        if (metricsEnabled()) {
            env.put(ENV_KEY, ENV_VALUE);
            LOGGER.info("Set metrics configuration: key = {}, " +
                    "value = {}", ENV_KEY, ENV_VALUE);
        }
    }
}
