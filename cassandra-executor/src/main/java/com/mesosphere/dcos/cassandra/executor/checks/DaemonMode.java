package com.mesosphere.dcos.cassandra.executor.checks;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.CassandraExecutor;

import java.util.Optional;

public class DaemonMode extends HealthCheck {

    private final CassandraExecutor executor;

    public static final String NAME = "daemonMode";

    @Inject
    public DaemonMode(final CassandraExecutor executor) {
        this.executor = executor;
    }

    @Override
    protected Result check() throws Exception {

        Optional<CassandraDaemonProcess> daemon =
                executor.getCassandraDaemon();

        if (daemon.isPresent()) {
            return (daemon.get().getMode() == CassandraMode.NORMAL) ?
                    Result.healthy() : Result.unhealthy("Cassandra Daemon " +
                    "mode is " + daemon.get().getMode());
        } else {
            return Result.unhealthy("Cassandra Daemon is " +
                    "not running");
        }

    }
}
