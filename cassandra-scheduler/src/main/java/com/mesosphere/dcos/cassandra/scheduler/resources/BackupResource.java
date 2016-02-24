package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPlanManager;
import org.apache.mesos.scheduler.plan.PlanManager;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/backup")
public class BackupResource {
    private final static String STATUS_STARTED = "started";
    private final static String MESSAGE_STARTED = "Started complete backup";

    private final static String STATUS_ALREADY_RUNNING = "already_running";
    private final static String MESSAGE_ALREADY_RUNNING = "An existing backup is already in progress";

    private BackupManager backupManager;

    @Inject
    public BackupResource(BackupManager backupManager) {
        this.backupManager = backupManager;
    }

    @PUT
    @Timed
    @Path("/start")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response start(StartBackupRequest request) {
        if (backupManager.canStartBackup()) {
            final BackupContext backupContext = BackupResource.from(request);
            backupManager.startBackup(backupContext);
            final StartBackupResponse response = new StartBackupResponse(STATUS_STARTED, MESSAGE_STARTED);
            return Response.ok(response).build();
        } else {
            // Send error back
            return Response.status(502).
                    entity(new StartBackupResponse(STATUS_ALREADY_RUNNING, MESSAGE_ALREADY_RUNNING))
                    .build();
        }
    }

    @GET
    @Timed
    @Path("/status")
    public Response status() {
        return Response.ok().build();
    }

    public static BackupContext from(StartBackupRequest request) {
        final BackupContext backupContext =
                new BackupContext();
        backupContext.setName(request.getName());
        backupContext.setExternalLocation(request.getExternalLocation());
        backupContext.setS3AccessKey(request.getS3AccessKey());
        backupContext.setS3SecretKey(request.getS3SecretKey());

        return backupContext;
    }
}
