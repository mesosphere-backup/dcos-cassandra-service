package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.codahale.metrics.annotation.Counted;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.ExecutorConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/config")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigurationResource {

    private final ConfigurationManager configuration;

    @Inject
    public ConfigurationResource(
            final ConfigurationManager configuration) {
        this.configuration = configuration;
    }

    @GET
    @Path("/cassandra")
    @Counted
    public CassandraConfig getCassandraConfig() {
        return this.configuration.getCassandraConfig();
    }

    @GET
    @Path("/executor")
    @Counted
    public ExecutorConfig getExecutorConfig() {
        return this.configuration.getExecutorConfig();
    }

    @GET
    @Path("/nodes")
    public int getServers() {
        return configuration.getServers();
    }

    @GET
    @Path("/seed-nodes")
    public int getSeeds() {
        return configuration.getSeeds();
    }


}
