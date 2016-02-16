package com.mesosphere.dcos.cassandra.executor.resources;

import com.codahale.metrics.annotation.Counted;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.CassandraExecutor;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Optional;

@Path("/v1/cassandra")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraDaemonController {

    private final CassandraExecutor executor;

    private final CassandraDaemonProcess getDaemon() {

        Optional<CassandraDaemonProcess> process = executor
                .getCassandraDaemon();

        if (!process.isPresent()) {
            throw new NotFoundException();
        } else return process.get();
    }

    @Inject
    public CassandraDaemonController(CassandraExecutor executor) {

        this.executor = executor;

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
