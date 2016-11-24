package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;

import javax.ws.rs.Path;

@Path("/v1/repair")
public class RepairResource extends ClusterTaskResource<RepairRequest, RepairContext> {

    @Inject
    public RepairResource(final RepairManager manager) {
        super(manager, "Repair");
    }
}
