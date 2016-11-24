package com.mesosphere.dcos.cassandra.scheduler.resources;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskRequest;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ClusterTaskResource<R extends ClusterTaskRequest, C extends ClusterTaskContext> {

    private final ClusterTaskRunner<R, C> runner;

    public ClusterTaskResource(final ClusterTaskManager<R, C> manager, final String taskName) {
        runner = new ClusterTaskRunner<>(manager, "Restore");
    }

    @PUT
    @Timed
    @Path("/start")
    public Response start(R request) {
        return runner.start(request);
    }

    @PUT
    @Timed
    @Path("/stop")
    public Response stop() {
        return runner.stop();
    }

}
