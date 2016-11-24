package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable.UpgradeSSTableManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/upgradesstable")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class UpgradeSSTableResource extends ClusterTaskResource<UpgradeSSTableRequest, UpgradeSSTableContext> {

    private final boolean enableUpgradeSSTableEndpoint;

    @Inject
    public UpgradeSSTableResource(
            final UpgradeSSTableManager manager,
            @Named("ConfiguredEnableUpgradeSSTableEndpoint") boolean enableUpgradeSSTableEndpoint) {
        super(manager, "UpgradeSSTable");
        this.enableUpgradeSSTableEndpoint = enableUpgradeSSTableEndpoint;
    }

    @Override
    @PUT
    @Timed
    @Path("/start")
    public Response start(UpgradeSSTableRequest request) {
        if (!enableUpgradeSSTableEndpoint) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity(ErrorResponse.fromString(
                            "Upgrading SSTable endpoint is not enabled. " +
                            "Please enable it by setting $ENABLE_UPGRADE_SSTABLE_ENDPOINT"))
                    .build();
        }
        return super.start(request);
    }
}
