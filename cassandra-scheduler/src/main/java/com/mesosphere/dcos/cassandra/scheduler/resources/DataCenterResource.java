package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

@Path("/v1/datacenter")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DataCenterResource {
    private final SeedsManager seeds;
    @Inject
    public DataCenterResource(final SeedsManager seeds){
        this.seeds = seeds;
    }

    @GET
    public DataCenterInfo getDcInfo() throws IOException {
        return seeds.getLocalInfo();
    }

    @PUT
    public void registerDcInfo(DataCenterInfo info) throws Exception{
        this.seeds.update(info);
    }
}
