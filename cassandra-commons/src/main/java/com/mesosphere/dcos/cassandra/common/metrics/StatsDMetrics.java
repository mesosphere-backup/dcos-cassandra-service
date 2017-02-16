package com.mesosphere.dcos.cassandra.common.metrics;

import com.mesosphere.dcos.cassandra.common.config.MetricConfig;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports StatsD Metrics.
 */
public class StatsDMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsDMetrics.class);

    StatsDClient statsd = null;
    private final MetricConfig config;

    public StatsDMetrics(MetricConfig config) {
        this.config = config;
        LOGGER.info("StatsD Metric Values, Prefix: {}, Host: {}, Port: {}, Enabled: {}", config.getPrefix(), config.getHost(), config.getPort(), config.getEnabled());
        statsd = new NonBlockingStatsDClient(config.getPrefix(), config.getHost(), config.getPort());
    }

    public void gauge(String gaugeName, int value) {
        if(config.getEnabled()) {
            statsd.gauge(gaugeName, value);
            LOGGER.info("Metric emitted for {}", gaugeName);
        }
    }
}
