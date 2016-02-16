package com.mesosphere.dcos.cassandra.scheduler.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

import java.util.List;

public class ServersCheck extends HealthCheck {
    public static final String NAME = "serverCount";

    private CassandraTasks tasks;
    private ConfigurationManager configurationManager;

    @Inject
    public ServersCheck(final CassandraTasks tasks, ConfigurationManager configurationManager) {
        this.tasks = tasks;
        this.configurationManager = configurationManager;
    }

    @Override
    protected Result check() throws Exception {
        final int servers = configurationManager.getServers();
        final List<CassandraTask> runningTasks = tasks.getRunningTasks();
        final int numRunningTasks = runningTasks.size();
        final String message = "Expected running tasks = " + servers + " actual = " + numRunningTasks;
        if (numRunningTasks == servers) {
            return Result.healthy(message);
        } else {
            return Result.unhealthy(message);
        }
    }
}
