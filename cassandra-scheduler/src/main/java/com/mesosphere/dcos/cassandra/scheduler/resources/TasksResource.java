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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.glassfish.jersey.server.ManagedAsync;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Path("/v1/nodes")
@Produces(MediaType.APPLICATION_JSON)
public class TasksResource {

    private final CassandraTasks tasks;
    private final IdentityManager id;
    private final SchedulerClient client;

    private List<CassandraDaemonTask> getRunningDeamons() {
        return tasks.getDaemons().values().stream()
                .filter
                        (daemonTask ->
                                Protos.TaskState.TASK_RUNNING.equals(
                                        daemonTask.getStatus().getState())).collect
                        (Collectors.toList());
    }

    private List<String> getRunningAddresses() {

        return getRunningDeamons().stream().map(daemonTask ->
                daemonTask.getHostname() +
                        ":" +
                        daemonTask.getConfig()
                                .getApplication()
                                .getNativeTransportPort())
                .collect(Collectors.toList());
    }

    private List<String> getRunningDns() {

        return getRunningDeamons().stream().map(daemonTask ->
                daemonTask.getName() +
                        "." + id.get().getName() + ".mesos:" +
                        daemonTask.getConfig()
                                .getApplication()
                                .getNativeTransportPort()
        ).collect(Collectors.toList());
    }

    @Inject
    public TasksResource(final CassandraTasks tasks,
                         final IdentityManager id,
                         final SchedulerClient client) {
        this.tasks = tasks;
        this.client = client;
        this.id = id;
    }

    @GET
    @Path("/list")
    public List<String> list() {
        return new ArrayList<>(tasks.getDaemons().keySet());
    }

    @GET
    @Path("/{name}/status")
    @ManagedAsync
    public void getStatus(
            @PathParam("name") final String name,
            @Suspended final AsyncResponse response) {

        Optional<CassandraDaemonTask> taskOption =
                Optional.ofNullable(tasks.getDaemons().get(name));
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
                Optional.ofNullable(tasks.getDaemons().get(name));
        if (taskOption.isPresent()) {
            return DaemonInfo.create(taskOption.get());
        } else {
            throw new NotFoundException();
        }
    }

    @PUT
    @Path("/restart")
    public void restart(@QueryParam("node") final String name) {
        Optional<CassandraDaemonTask> taskOption =
                Optional.ofNullable(tasks.getDaemons().get(name));
        if (taskOption.isPresent()) {
            CassandraDaemonTask task = taskOption.get();
            client.shutdown(task.getHostname(),
                    task.getExecutor().getApiPort());
        } else {
            throw new NotFoundException();
        }
    }

    @PUT
    @Path("/replace")
    public void replace(@QueryParam("node") final String name)
            throws Exception {
        Optional<CassandraDaemonTask> taskOption =
                Optional.ofNullable(tasks.getDaemons().get(name));
        if (taskOption.isPresent()) {
            CassandraDaemonTask task = taskOption.get();
            tasks.moveDaemon(task);
            if (!TaskUtils.isTerminated(task.getStatus().getState())) {
                client.shutdown(task.getHostname(),
                        task.getExecutor().getApiPort());
            }
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("connect")
    public Map<String, List<String>> connect() {

        return ImmutableMap.of("address", getRunningAddresses(),
                "dns", getRunningDns());
    }

    @GET
    @Path("connect/address")
    public List<String> connectAddress() {
        return getRunningAddresses();
    }

    @GET
    @Path("connect/dns")
    public List<String> connectDns() {
        return getRunningDns();
    }
}
