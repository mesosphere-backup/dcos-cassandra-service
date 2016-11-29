package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/backup")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class BackupResource {

    private final ClusterTaskRunner<BackupRestoreRequest, BackupRestoreContext> runner;

    public BackupResource(final BackupManager manager) {
        this.runner = new ClusterTaskRunner<>(manager, "Backup");
    }

    @PUT
    @Timed
    @Path("start")
    public Response start(BackupRestoreRequest request) {
        return runner.start(request);
    }

    @PUT
    @Timed
    @Path("stop")
    public Response stop() {
        return runner.stop();
    }
}
