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

    @Inject
    public CassandraDaemonController(Executor executor) {

        LOGGER.info("Setting executor to {}",executor);

        this.executor = (CassandraExecutor) executor;

        LOGGER.info("Set executor to {}",this.executor);
    }

    @GET
    @Counted
    @Path("/status")
    public CassandraStatus getStatus() {

        return getDaemon().getStatus();
    }

    @GET
    @Counted
    @Path("/configuration")
    public CassandraConfig getConfig() {

        return getDaemon().getTask().getConfig();
    }

    @DELETE
    @Counted
    public void shutdown(){

        getDaemon().shutdown();
    }


}
