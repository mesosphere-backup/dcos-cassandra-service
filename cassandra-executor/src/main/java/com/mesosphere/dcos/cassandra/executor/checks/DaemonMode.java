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
package com.mesosphere.dcos.cassandra.executor.checks;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.CassandraExecutor;

import java.util.Optional;

/**
 * DaemonMode implements a health check that tests if the Cassandra Daemon's
 * mode is normal.
 */
public class DaemonMode extends HealthCheck {

    private final CassandraExecutor executor;

    /**
     * The name of the health check.
     */
    public static final String NAME = "daemonMode";

    /**
     * Constructs a new DeamonMode check.
     * @param executor The CassandraExecutor instance that will be checked.
     */
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
