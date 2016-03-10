package com.mesosphere.dcos.cassandra.executor.metrics;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private MetricsConfig() {}

    public static boolean metricsEnabled(){
        return System.getenv(STATSD_HOST_ENV) != null &&
                System.getenv(STATSD_PORT_ENV) != null;
    }

    public static boolean writeMetricsConfig(final Path dir)  {
        if(!metricsEnabled()){
            LOGGER.info("Metrics is not enabled");
            return false;
        }

        String host = System.getenv(STATSD_HOST_ENV);

        int port;
        try{
            port = Integer.parseInt(System.getenv(STATSD_PORT_ENV));
        } catch(NumberFormatException ne){
            LOGGER.error("Failed to parse port parameter",ne);
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
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(metricsYaml))) {
            yaml.dump(yamlMap, bw);
        } catch(IOException ex){
            LOGGER.error("Failed to write configuration",ex);
            return false;
        }
        LOGGER.info("Wrote {}",CONFIG_FILE);
        return true;
    }

    public static void setEnv(final Map<String,String> env){
        if(metricsEnabled()){
            env.put(ENV_KEY,ENV_VALUE);
            LOGGER.info("Set metrics configuration: key = {}, " +
                    "value = {}",ENV_KEY,ENV_VALUE);
        }
    }
}
