package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.ServiceConfig;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/v1/connect")
@Produces(MediaType.APPLICATION_JSON)
public class ConnectionResource {
    private final CassandraTasks tasks;
    private final ConfigurationManager configurationManager;

    @Inject
    public ConnectionResource(final CassandraTasks tasks,
                              final ConfigurationManager configurationManager) {
        this.tasks = tasks;
        this.configurationManager = configurationManager;
    }

    @GET
    public Map<String, List<String>> connect() throws ConfigStoreException {

        return ImmutableMap.of("address", getRunningAddresses(),
                "dns", getRunningDns());
    }

    @GET
    @Path("/address")
    public List<String> connectAddress() {
        return getRunningAddresses();
    }

    @GET
    @Path("/dns")
    public List<String> connectDns() throws ConfigStoreException {
        return getRunningDns();
    }

    @VisibleForTesting
    protected List<String> getRunningAddresses() {

        return getRunningDeamons().stream().map(daemonTask ->
                daemonTask.getHostname() +
                        ":" +
                        daemonTask.getConfig()
                                .getApplication()
                                .getNativeTransportPort())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected List<String> getRunningDns() throws ConfigStoreException {
        final ServiceConfig serviceConfig = configurationManager.getTargetConfig().getServiceConfig();
        return getRunningDeamons().stream().map(daemonTask ->
                daemonTask.getName() +
                        "." + serviceConfig.getName() + ".mesos:" +
                        daemonTask.getConfig()
                                .getApplication()
                                .getNativeTransportPort()
        ).collect(Collectors.toList());
    }

    @VisibleForTesting
    protected List<CassandraDaemonTask> getRunningDeamons() {
        return tasks.getDaemons().values().stream()
                .filter
                        (daemonTask ->
                                Protos.TaskState.TASK_RUNNING.equals(
                                        daemonTask.getState())).collect
                        (Collectors.toList());
    }
}
