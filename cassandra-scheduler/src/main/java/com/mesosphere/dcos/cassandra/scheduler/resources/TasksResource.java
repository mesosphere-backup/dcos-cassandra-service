/*
 * Copyright 2015 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.inject.Inject;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.CassandraScheduler;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.dcos.Capabilities;
import org.glassfish.jersey.server.ManagedAsync;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("/v1/nodes")
@Produces(MediaType.APPLICATION_JSON)
public class TasksResource {
    private static final Logger LOGGER = LoggerFactory.getLogger
            (TasksResource.class);
    private final Capabilities capabilities;
    private final CassandraState state;
    private final SchedulerClient client;
    private final ConfigurationManager configurationManager;

    @Inject
    public TasksResource(
            final Capabilities capabilities,
            final CassandraState state,
            final SchedulerClient client,
            final ConfigurationManager configurationManager) {
        this.capabilities = capabilities;
        this.state = state;
        this.client = client;
        this.configurationManager = configurationManager;
    }

    @GET
    @Path("/list")
    public List<String> list() {
        return new ArrayList<>(state.getDaemons().keySet());
    }

    @GET
    @Path("/{name}/status")
    @ManagedAsync
    public void getStatus(
        @PathParam("name") final String name,
        @Suspended final AsyncResponse response) {

        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (!taskOption.isPresent()) {
            response.resume(
                Response.status(Response.Status.NOT_FOUND));
        } else {
            CassandraDaemonTask task = taskOption.get();
            client.status(task.getHostname(), task.getExecutor().getApiPort()
            ).whenCompleteAsync((status, error) -> {
                if (status != null) {
                    response.resume(status);
                } else {
                    response.resume(Response.serverError());
                }
            });
        }
    }

    @GET
    @Path("/{name}/info")
    public DaemonInfo getInfo(@PathParam("name") final String name) {

        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (taskOption.isPresent()) {
            return DaemonInfo.create(taskOption.get());
        } else {
            throw new NotFoundException();
        }
    }

    @PUT
    @Path("/restart")
    public Response restart(@QueryParam("node") final String name) {
        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (taskOption.isPresent()) {
            CassandraDaemonTask task = taskOption.get();
            CassandraScheduler.getTaskKiller().killTask(task.getName(), false);
            return killResponse(Arrays.asList(task.getTaskInfo().getTaskId().getValue()));
        } else {
            return Response.serverError().build();
        }
    }

    @PUT
    @Path("/replace")
    public Response replace(@QueryParam("node") final String name)
        throws Exception {
        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (taskOption.isPresent()) {
            CassandraDaemonTask task = taskOption.get();
            final CassandraContainer movedContainer = state.moveCassandraContainer(task);
            LOGGER.info("Moved container ExecutorInfo: {}",
                    TextFormat.shortDebugString(movedContainer.getExecutorInfo()));
            CassandraScheduler.getTaskKiller().killTask(task.getName(), true);
            return killResponse(Arrays.asList(task.getTaskInfo().getTaskId().getValue()));
        } else {
            return Response.serverError().build();
        }
    }

    /**
     * Deprecated. See {@code ConnectionResource}.
     */
    @GET
    @Path("connect")
    @Deprecated
    public Map<String, Object> connect() throws ConfigStoreException {
        final ConnectionResource connectionResource =
                new ConnectionResource(capabilities, state, configurationManager);
        return connectionResource.connect();
    }

    /**
     * Deprecated. See {@code ConnectionResource}.
     */
    @GET
    @Path("connect/address")
    @Deprecated
    public List<String> connectAddress() {
        final ConnectionResource connectionResource =
                new ConnectionResource(capabilities, state, configurationManager);
        return connectionResource.connectAddress();
    }

    /**
     * Deprecated. See {@code ConnectionResource}.
     */
    @GET
    @Path("connect/dns")
    @Deprecated
    public List<String> connectDns() throws ConfigStoreException {
        final ConnectionResource connectionResource =
                new ConnectionResource(capabilities, state, configurationManager);
        return connectionResource.connectDns();
    }

    private static Response killResponse(List<String> taskIds) {
        return Response.ok(new JSONArray(taskIds).toString(), MediaType.APPLICATION_JSON).build();
    }
}
