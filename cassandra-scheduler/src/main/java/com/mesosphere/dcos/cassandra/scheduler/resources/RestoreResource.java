package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/restore")
public class RestoreResource {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RestoreResource.class);

    private final RestoreManager manager;

    @Inject
    public RestoreResource(final RestoreManager manager) {
        this.manager = manager;
    }

    @PUT
    @Timed
    @Path("/start")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response start(StartRestoreRequest request) {
        LOGGER.info("Processing restore request: request = {}", request);
        try {
            if(!request.isValid()){
                return Response.status(Response.Status.BAD_REQUEST).build();
            } else if (manager.canStartRestore()) {
                final RestoreContext context = from(request);
                manager.startRestore(context);
                LOGGER.info("Started restore: context = {}", context);
                return Response.accepted().build();
            } else {
                // Send error back
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(ErrorResponse.fromString(
                                "Restore already in progress."))
                        .build();
            }
        } catch (Throwable throwable) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(ErrorResponse.fromThrowable(throwable))
                    .build();
        }
    }

    public static RestoreContext from(StartRestoreRequest request) {
        final RestoreContext context =
                new RestoreContext();
        context.setName(request.getName());
        context.setExternalLocation(request.getExternalLocation());
        context.setS3AccessKey(request.getS3AccessKey());
        context.setS3SecretKey(request.getS3SecretKey());
        return context;
    }
}
