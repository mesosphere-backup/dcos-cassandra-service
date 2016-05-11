package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Counted;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

@Path("/v1/seeds")
@Produces(MediaType.APPLICATION_JSON)
public class SeedsResource {

    private final SeedsManager seeds;

    @Inject
    public SeedsResource(SeedsManager seeds) {
        this.seeds = seeds;
    }

    @GET
    @Counted
    public SeedsResponse getSeeds() throws IOException {
      return seeds.getSeeds();
    }
}
