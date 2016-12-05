package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/cleanup")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CleanupResource {

    private final ClusterTaskRunner<CleanupRequest, CleanupContext> runner;

    public CleanupResource(final CleanupManager manager) {
        this.runner = new ClusterTaskRunner<>(manager, "Cleanup");
    }

    @PUT
    @Timed
    @Path("start")
    public Response start(CleanupRequest request) {
        return runner.start(request);
    }

    @PUT
    @Timed
    @Path("stop")
    public Response stop() {
        return runner.stop();
    }
}
