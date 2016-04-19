package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

@Path("/v1/cleanup")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CleanupResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            CleanupResource.class);
    private final CleanupManager manager;
    private final CassandraTasks tasks;

    @Inject
    public CleanupResource(final CleanupManager manager,
                           final CassandraTasks tasks) {
        this.manager = manager;
        this.tasks = tasks;
    }

    @PUT
    @Timed
    @Path("/start")
    public Response start(CleanupRequest request) {
        LOGGER.info("Processing start cleanup request = {}", request);
        try {
            if(!request.isValid()){
                return Response.status(Response.Status.BAD_REQUEST).build();
            }  else if (manager.canStartCleanup()) {
                manager.startCleanup(CleanupContext.create(
                       new ArrayList<>(getNodes(request)),
                        request.getKeySpaces(),
                        request.getColumnFamiles()
                ));

                LOGGER.info("Cleanup started");
                return Response.accepted().build();
            } else {
                // Send error back
                LOGGER.warn("Cleanup already in progress: request = {}",
                        request);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(ErrorResponse.fromString(
                                "Cleanup already in progress"))
                        .build();
            }
        } catch (Throwable t) {
            LOGGER.error(
                    String.format("Error creating cleanup: request = %s",
                            request), t);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(ErrorResponse.fromThrowable(t))
                    .build();
        }
    }

    private Set<String> getNodes(CleanupRequest request) {
        if (request.getNodes().size() == 1 &&
                request.getNodes().get(0).equals(CleanupRequest.ALL)) {
            return tasks.getDaemons().keySet();
        } else {
            final Set<String> daemons = tasks.getDaemons().keySet();
            return request.getNodes().stream().filter(node -> daemons
                    .contains(node)).collect(Collectors.toSet());
        }

    }
}
