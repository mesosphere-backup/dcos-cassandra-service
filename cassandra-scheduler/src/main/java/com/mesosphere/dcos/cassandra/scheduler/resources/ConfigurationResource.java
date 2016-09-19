package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.codahale.metrics.annotation.Counted;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import org.apache.mesos.config.ConfigStoreException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/config")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigurationResource {

    private final DefaultConfigurationManager configurationManager;

    @Inject
    public ConfigurationResource(
            final DefaultConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    @GET
    @Path("/cassandra")
    @Counted
    public CassandraConfig getCassandraConfig() throws ConfigStoreException {
        return ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getCassandraConfig();
    }

    @GET
    @Path("/executor")
    @Counted
    public ExecutorConfig getExecutorConfig() throws ConfigStoreException {
        return ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getExecutorConfig();
    }

    @GET
    @Path("/nodes")
    public int getServers() throws ConfigStoreException {
        return ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getServers();
    }

    @GET
    @Path("/seed-nodes")
    public int getSeeds() throws ConfigStoreException {
        return ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getSeeds();
    }


}
