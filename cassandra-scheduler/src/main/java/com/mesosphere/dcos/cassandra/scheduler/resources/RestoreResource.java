package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;

import javax.ws.rs.Path;

@Path("/v1/restore")
public class RestoreResource extends ClusterTaskResource<BackupRestoreRequest, BackupRestoreContext> {

    @Inject
    public RestoreResource(final RestoreManager manager) {
        super(manager, "Restore");
    }
}
