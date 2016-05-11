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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
    private static final int STATSD_FLUSH_PERIOD = 10;
    private static final String STATSD_FLUSH_PERIOD_UNIT = "SECONDS";

    private MetricsConfig() {
    }

    /***
     * Tests if metrics collection is enabled.
     * @return True if metrics collection is enabled.
     */
    public static boolean metricsEnabled() {
        return System.getenv(STATSD_HOST_ENV) != null &&
                System.getenv(STATSD_PORT_ENV) != null;
    }

    /**
     * Writes the metrics configuration file.
     * @param dir The directory where the configuration file will be written.
     * @return True if the metrics configuration file was written.
     */
    public static boolean writeMetricsConfig(final Path dir) {
        if (!metricsEnabled()) {
            LOGGER.info("Metrics is not enabled");
            return false;
        }

        String host = System.getenv(STATSD_HOST_ENV);

        int port;
        try {
            port = Integer.parseInt(System.getenv(STATSD_PORT_ENV));
        } catch (NumberFormatException ne) {
            LOGGER.error("Failed to parse port parameter", ne);
            return false;
        }

        LOGGER.info("Building {}", CONFIG_FILE);
        Map<String, Object> hostMap = new HashMap<>();
        hostMap.put("host", host);
        hostMap.put("port", port);
        List<Object> hostMapList = new ArrayList<>();
        hostMapList.add(hostMap);

        Map<String, Object> statsdMap = new HashMap<>();
        statsdMap.put("period", STATSD_FLUSH_PERIOD);
        statsdMap.put("timeunit", STATSD_FLUSH_PERIOD_UNIT);
        statsdMap.put("hosts", hostMapList);

        List<Object> statsdMapList = new ArrayList<>();
        statsdMapList.add(statsdMap);
        final Map<String, Object> yamlMap = new HashMap<>();
        yamlMap.put("statsd", statsdMapList);
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
    public static void setEnv(final Map<String, String> env) {
        if (metricsEnabled()) {
            env.put(ENV_KEY, ENV_VALUE);
            LOGGER.info("Set metrics configuration: key = {}, " +
                    "value = {}", ENV_KEY, ENV_VALUE);
        }
    }
}
