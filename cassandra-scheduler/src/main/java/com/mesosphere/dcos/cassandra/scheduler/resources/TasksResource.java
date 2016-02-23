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
import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.glassfish.jersey.server.ManagedAsync;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Path("/v1/nodes")
@Produces(MediaType.APPLICATION_JSON)
public class TasksResource {

    private final CassandraTasks tasks;
    private final ExecutorClient client;

    @Inject
    public TasksResource(final CassandraTasks tasks,
                         final ExecutorClient client) {

        this.tasks = tasks;
        this.client = client;

    }

    @GET
    @Path("/list")
    public List<DaemonInfo> list() {
        return tasks.getDaemons().values().stream().map(DaemonInfo::create)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/status/{id}")
    @ManagedAsync
    public void getStatus(
            @PathParam("id") final String id,
            @Suspended final AsyncResponse response) {
        Optional<CassandraTask> taskOption = tasks.get(id);
        if (!taskOption.isPresent()) {
            response.resume(
                    Response.status(Response.Status.NOT_FOUND));
        } else {
            CassandraDaemonTask task = (CassandraDaemonTask) taskOption.get();
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
}
