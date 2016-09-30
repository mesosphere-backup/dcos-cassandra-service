package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.common.collect.ImmutableMap;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.config.ServiceConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.dcos.Capabilities;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/v1/connection")
@Produces(MediaType.APPLICATION_JSON)
public class ConnectionResource {
    private final Capabilities capabilities;
    private final CassandraState state;
    private final ConfigurationManager configurationManager;

    @Inject
    public ConnectionResource(
            final Capabilities capabilities,
            final CassandraState state,
            final ConfigurationManager configurationManager) {
        this.capabilities = capabilities;
        this.state = state;
        this.configurationManager = configurationManager;
    }

    @GET
    public Map<String, Object> connect() throws ConfigStoreException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("address", connectAddress());
        builder.put("dns", connectDns());
        try {
            if (capabilities.supportsNamedVips()) {
                builder.put("vip", toVip(configurationManager.getTargetConfig().getServiceConfig()));
            }
        } catch (Exception e) {
            // assume unavailable
        }
        return builder.build();
    }

    @GET
    @Path("/address")
    public List<String> connectAddress() {
        return toRunningAddresses(state.getDaemons());
    }

    @GET
    @Path("/dns")
    public List<String> connectDns() throws ConfigStoreException {
        return toRunningDns(
                state.getDaemons(), configurationManager.getTargetConfig().getServiceConfig());
    }

    private List<String> toRunningAddresses(
            final Map<String, CassandraDaemonTask> daemons) {
        return toRunningDaemons(daemons).stream()
                .map(daemonTask -> String.format(
                        "%s:%d",
                        daemonTask.getHostname(),
                        daemonTask.getConfig().getApplication().getNativeTransportPort()))
                .collect(Collectors.toList());
    }

    private static List<String> toRunningDns(
            final Map<String, CassandraDaemonTask> daemons,
            final ServiceConfig serviceConfig) {
        return toRunningDaemons(daemons).stream()
                .map(daemonTask -> String.format(
                        "%s.%s.mesos:%d",
                        daemonTask.getName(),
                        serviceConfig.getName(),
                        daemonTask.getConfig().getApplication().getNativeTransportPort()))
                .collect(Collectors.toList());
    }

    private static String toVip(ServiceConfig serviceConfig) {
        return String.format(
                "%s.%s.l4lb.thisdcos.directory:%d",
                CassandraDaemonTask.VIP_NODE_NAME,
                serviceConfig.getName(),
                CassandraDaemonTask.VIP_NODE_PORT);
    }

    private static List<CassandraDaemonTask> toRunningDaemons(
            final Map<String, CassandraDaemonTask> daemons) {
        return daemons.values().stream()
                .filter(daemonTask -> Protos.TaskState.TASK_RUNNING.equals(daemonTask.getState()))
                .collect(Collectors.toList());
    }
}
