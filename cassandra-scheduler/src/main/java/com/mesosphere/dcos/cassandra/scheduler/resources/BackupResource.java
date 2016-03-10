package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.backup.BackupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/backup")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class BackupResource {
    private static final Logger LOGGER = LoggerFactory.getLogger
            (BackupResource.class);
    private final static String STATUS_STARTED = "started";
    private final static String MESSAGE_STARTED = "Started complete backup";

    private final static String STATUS_ALREADY_RUNNING = "already_running";
    private final static String MESSAGE_ALREADY_RUNNING = "An existing backup is already in progress";

    private BackupManager manager;

    @Inject
    public BackupResource(BackupManager manager) {
        this.manager = manager;
    }

    @PUT
    @Timed
    @Path("/start")
    public Response start(StartBackupRequest request) {
        LOGGER.info("Processing start backup request = {}", request);
        try {
            if (manager.canStartBackup()) {
                final BackupContext backupContext = from(request);
                manager.startBackup(backupContext);
                final StartBackupResponse response = new StartBackupResponse(
                        STATUS_STARTED, MESSAGE_STARTED);
                LOGGER.info("Backup response = {}",response);
                return Response.ok(response).build();
            } else {
                // Send error back
                LOGGER.warn("Backup already in progress: request = {}",request);
                return Response.status(502).
                        entity(new StartBackupResponse(STATUS_ALREADY_RUNNING,
                                MESSAGE_ALREADY_RUNNING))
                        .build();
            }
        } catch (Throwable t) {
            LOGGER.error(
                    String.format("Error creating backup: request = %s",
                            request), t);
            return Response.status(500).entity(
                    ImmutableMap.of("error", t.getMessage()))
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
