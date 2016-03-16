package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Counted;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/framework")
@Produces(MediaType.APPLICATION_JSON)
public class IdentityResource {

    private final IdentityManager manager;

    @Inject
    public IdentityResource(final IdentityManager manager) {
        this.manager = manager;
    }

    @GET
    @Counted
    public Identity getIdentity() {
        return manager.get();
    }

}
