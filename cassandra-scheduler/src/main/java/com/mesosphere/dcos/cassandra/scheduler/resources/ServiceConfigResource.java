package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Counted;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.config.ServiceConfig;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/framework")
@Produces(MediaType.APPLICATION_JSON)
public class ServiceConfigResource {

    private final ConfigurationManager configurationManager;

    @Inject
    public ServiceConfigResource(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    @GET
    @Counted
    public ServiceConfig getIdentity() throws Exception {
        return configurationManager.getTargetConfig().getServiceConfig();
    }

}
