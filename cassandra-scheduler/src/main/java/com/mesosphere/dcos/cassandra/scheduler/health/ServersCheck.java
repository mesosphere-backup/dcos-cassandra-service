package com.mesosphere.dcos.cassandra.scheduler.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

import java.util.List;
import java.util.stream.Collectors;

public class ServersCheck extends HealthCheck {
    public static final String NAME = "serverCount";

    private CassandraTasks tasks;

    @Inject
    public ServersCheck(final CassandraTasks tasks) {
        this.tasks = tasks;
    }

    @Override
    protected Result check() throws Exception {
        List<String> terminated = tasks.getDaemons().values()
                              .stream()
                               .filter(task -> task.isTerminated())
                               .map(task -> task.getName())
                               .collect(Collectors.toList());
                       return terminated.isEmpty() ?
                               Result.healthy("All Cassandra nodes running") :
                               Result.unhealthy("Unhealthy nodes = " +
                                       Joiner.on(",").join(terminated));
    }
}
