package com.mesosphere.dcos.cassandra.common.metrics;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.MetricConfig;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

/**
 * Reports StatsD Metrics.
 */
public class StatsDMetrics {

    StatsDClient statsd = null;
    private final MetricConfig config;

    @Inject
    public StatsDMetrics(MetricConfig config) {
        this.config = config;
        statsd = new NonBlockingStatsDClient(config.getPrefix(), config.getHost(), config.getPort());
    }

    public void gauge(String gaugeName, int value) {
        if(config.getEnabled()) {
            statsd.gauge(gaugeName, value);
        }
    }
}
