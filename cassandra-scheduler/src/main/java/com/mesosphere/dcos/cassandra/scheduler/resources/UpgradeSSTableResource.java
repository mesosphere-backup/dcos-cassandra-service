package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable.UpgradeSSTableManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/upgradesstable")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class UpgradeSSTableResource {

    private final ClusterTaskRunner<UpgradeSSTableRequest> runner;

    @Inject
    public UpgradeSSTableResource(final UpgradeSSTableManager manager) {
        runner = new ClusterTaskRunner<>(manager, "UpgradeSSTable");
    }

    @PUT
    @Timed
    @Path("/start")
    public Response start(UpgradeSSTableRequest request) {
        return runner.start(request);
    }

    @PUT
    @Timed
    @Path("/stop")
    public Response stop() {
        return runner.stop();
    }
}
