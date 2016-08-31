package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/restore")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RestoreResource {

    private final ClusterTaskRunner<BackupRestoreRequest> runner;

    @Inject
    public RestoreResource(final RestoreManager manager) {
        runner = new ClusterTaskRunner<>(manager, "Restore");
    }

    @PUT
    @Timed
    @Path("/start")
    public Response start(BackupRestoreRequest request) {
        return runner.start(request);
    }

    @PUT
    @Timed
    @Path("/stop")
    public Response stop() {
        return runner.stop();
    }
}
