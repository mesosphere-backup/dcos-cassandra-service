package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;

import javax.ws.rs.Path;

@Path("/v1/cleanup")
public class CleanupResource extends ClusterTaskResource<CleanupRequest, CleanupContext> {

    @Inject
    public CleanupResource(final CleanupManager manager) {
        super(manager, "Cleanup");
    }
}
