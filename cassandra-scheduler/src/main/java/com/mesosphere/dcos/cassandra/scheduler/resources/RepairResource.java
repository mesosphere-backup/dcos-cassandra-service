package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
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

@Path("/v1/repair")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RepairResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            RepairResource.class);
    private final RepairManager manager;
    private final CassandraTasks tasks;

    @Inject
    public RepairResource(final RepairManager manager,
                          final CassandraTasks tasks) {
        this.manager = manager;
        this.tasks = tasks;
    }

    @PUT
    @Timed
    @Path("/start")
    public Response start(RepairRequest request) {
        LOGGER.info("Processing start repair request = {}", request);
        try {
            if (!request.isValid()) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            } else if (manager.canStartRepair()) {

                manager.startRepair(RepairContext.create(
                        new ArrayList<>(
                                getNodes(request)),
                        request.getKeySpaces(),
                        request.getColumnFamiles()
                ));

                LOGGER.info("Repair started : ");
                return Response.accepted().build();
            } else {
                // Send error back
                LOGGER.warn("Repair already in progress: request = {}",
                        request);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(ErrorResponse.fromString(
                                "Repair already in progress"))
                        .build();
            }
        } catch (Throwable t) {
            LOGGER.error(
                    String.format("Error creating repair: request = %s",
                            request), t);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(ErrorResponse.fromThrowable(t))
                    .build();
        }
    }

    private Set<String> getNodes(RepairRequest request) {
        if (request.getNodes().size() == 1 &&
                request.getNodes().get(0).equals(RepairRequest.ALL)) {
            return tasks.getDaemons().keySet();
        } else {
            final Set<String> daemons = tasks.getDaemons().keySet();
            return request.getNodes().stream().filter(node -> daemons
                    .contains(node)).collect(Collectors.toSet());
        }

    }
}
