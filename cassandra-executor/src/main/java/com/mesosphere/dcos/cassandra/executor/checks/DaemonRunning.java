package com.mesosphere.dcos.cassandra.executor.checks;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.CassandraExecutor;

import java.util.Optional;


public class DaemonRunning extends HealthCheck {

    public static final String NAME = "daemonRunning";

    private final CassandraExecutor executor;


    @Inject
    public DaemonRunning(final CassandraExecutor executor){
        this.executor = executor;
    }

    @Override
    protected Result check() throws Exception {

        Optional<CassandraDaemonProcess> daemon =
                executor.getCassandraDaemon();

        if(daemon.isPresent()){
            return (daemon.get().isOpen()) ?
                    Result.healthy() : Result.unhealthy("Cassandra Daemon is " +
                    "not running");
        } else {
            return Result.unhealthy("Cassandra Daemon is " +
                    "not running");
        }

    }
}
