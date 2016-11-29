package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/repair")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RepairResource {

    private final ClusterTaskRunner<RepairRequest, RepairContext> runner;

    public RepairResource(final RepairManager manager) {
        this.runner = new ClusterTaskRunner<>(manager, "Repair");
    }

    @PUT
    @Timed
    @Path("start")
    public Response start(RepairRequest request) {
        return runner.start(request);
    }

    @PUT
    @Timed
    @Path("stop")
    public Response stop() {
        return runner.stop();
    }
}
