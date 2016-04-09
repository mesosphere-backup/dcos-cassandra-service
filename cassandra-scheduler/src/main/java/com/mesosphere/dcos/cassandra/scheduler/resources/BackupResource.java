package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger
            (BackupResource.class);

    private final BackupManager manager;

    @Inject
    public BackupResource(final BackupManager manager) {
        this.manager = manager;
    }

    @PUT
    @Timed
    @Path("/start")
    public Response start(StartBackupRequest request) {
        LOGGER.info("Processing start backup request = {}", request);
        try {
            if(!request.isValid()){
                return Response.status(Response.Status.BAD_REQUEST).build();
            } else  if (manager.canStartBackup()) {
                final BackupContext backupContext = from(request);
                manager.startBackup(backupContext);
                LOGGER.info("Backup started : context = {}", backupContext);
                return Response.accepted().build();
            } else {
                // Send error back
                LOGGER.warn("Backup already in progress: request = {}",
                        request);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(
                                ErrorResponse.fromString(
                                        "Backup already in progress."))
                        .build();
            }
        } catch (Throwable t) {
            LOGGER.error(
                    String.format("Error creating backup: request = %s",
                            request), t);
            return Response.status(500).entity(
                    ErrorResponse.fromThrowable(t))
                    .build();
        }
    }

    public static BackupContext from(StartBackupRequest request) {
        final BackupContext context =
                new BackupContext();
        context.setName(request.getName());
        context.setExternalLocation(request.getExternalLocation());
        context.setS3AccessKey(request.getS3AccessKey());
        context.setS3SecretKey(request.getS3SecretKey());

        return context;
    }
}
