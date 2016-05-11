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
package com.mesosphere.dcos.cassandra.executor.resources;

import com.codahale.metrics.annotation.Counted;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.CassandraExecutor;
import org.apache.mesos.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Optional;

/**
 * CassandraDaemonController implements the API for remote controll of the
 * Cassandra daemon process from the scheduler.
 */
@Path("/v1/cassandra")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraDaemonController {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CassandraDaemonController.class);

    private final CassandraExecutor executor;

    private final CassandraDaemonProcess getDaemon() {

        Optional<CassandraDaemonProcess> process = executor
                .getCassandraDaemon();

        if (!process.isPresent()) {
            throw new NotFoundException();
        } else return process.get();
    }

    /**
     * Constructs a new controller.
     * @param executor The Executor instance that will be controlled.
     */
    @Inject
    public CassandraDaemonController(Executor executor) {

        LOGGER.info("Setting executor to {}", executor);
        this.executor = (CassandraExecutor) executor;
        LOGGER.info("Set executor to {}", this.executor);
    }

    /**
     * Gets the status of the Cassandra process.
     * @return A CassandraStatus object containing the status of the
     * Cassandra process.
     */
    @GET
    @Counted
    @Path("/status")
    public CassandraStatus getStatus() {

        return getDaemon().getStatus();
    }

    /**
     * Gets the configuration of the Cassandra daemon.
     * @return A CassandraConfig object containing the configuration of the
     * Cassandra daemon.
     */
    @GET
    @Counted
    @Path("/configuration")
    public CassandraConfig getConfig() {

        return getDaemon().getTask().getConfig();
    }

    /**
     * Shuts the Cassandra daemon down. This will also shutdown the executor
     * instance via a side effect.
     */
    @DELETE
    @Counted
    public void shutdown() {

        getDaemon().shutdown();
    }


}
