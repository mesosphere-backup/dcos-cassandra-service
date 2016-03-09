package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Counted;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/v1/seeds")
@Produces(MediaType.APPLICATION_JSON)
public class SeedsResource {

    private final CassandraTasks tasks;
    private final ConfigurationManager configuration;

    @Inject
    public SeedsResource(final CassandraTasks tasks,
                         final ConfigurationManager configuration){
        this.tasks = tasks;
        this.configuration = configuration;
    }

    @GET
    @Counted
    public Map<String,Object> getSeeds() throws UnknownHostException {
        final List<CassandraDaemonTask> active = tasks.getDaemons().values()
                .stream()
                .filter(daemon -> daemon.getStatus().getMode() ==
                        CassandraMode.NORMAL &&
                daemon.getHostname().isEmpty() == false).collect(Collectors
                        .toList());

        final int seedCount = configuration.getSeeds();
        final List<String> seeds = new ArrayList<>(active.size());

        for(int seed = 0; seed < seedCount && seed < active.size(); ++seed){

            seeds.add(InetAddress.getByName(active.get(seed).getHostname())
                    .getHostAddress());
        }

        Map<String,Object> response = new HashMap<>();
        response.put("isSeed",(seeds.size() < seedCount) ? true : false);
        response.put("seeds",seeds);

        return response;

    }
}
