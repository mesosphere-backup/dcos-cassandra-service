package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;

import javax.ws.rs.Path;

@Path("/v1/backup")
public class BackupResource extends ClusterTaskResource<BackupRestoreRequest, BackupRestoreContext> {

    @Inject
    public BackupResource(final BackupManager manager) {
        super(manager, "Backup");
    }
}
