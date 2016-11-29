package com.mesosphere.dcos.cassandra.scheduler.resources;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.mesos.config.SerializationUtils;
import org.glassfish.jersey.process.Inflector;
import org.glassfish.jersey.server.model.Resource;

import com.codahale.metrics.annotation.Timed;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskRequest;

public class ClusterTaskResourceFactory {
    private ClusterTaskResourceFactory() {
        // do not instantiate
    }

    public static <R extends ClusterTaskRequest, C extends ClusterTaskContext> Resource create(
            String taskName, String path, final ClusterTaskManager<R, C> manager, Class<R> requestClass) {
        Resource.Builder resourceBuilder = Resource.builder()
                .name(taskName)
                .path(path);

        final ClusterTaskRunner<R, C> runner = new ClusterTaskRunner<>(manager, taskName);

        resourceBuilder.addChildResource("start").addMethod("PUT")
                .produces(MediaType.APPLICATION_JSON)
                .consumes(MediaType.APPLICATION_JSON)
                .handledBy(new Inflector<ContainerRequestContext, Response>() {
                    @Override
                    @Timed
                    public Response apply(ContainerRequestContext context) {
                        try {
                            return runner.start(SerializationUtils.fromJsonString(
                                    IOUtils.toString(context.getEntityStream()),
                                    requestClass));
                        } catch (IOException e) {
                            e.printStackTrace();
                            return Response.status(Response.Status.BAD_REQUEST).build();
                        }
                    }
                });

        resourceBuilder.addChildResource("stop").addMethod("PUT")
                .produces(MediaType.APPLICATION_JSON)
                .consumes(MediaType.APPLICATION_JSON)
                .handledBy(new Inflector<ContainerRequestContext, Response>() {
                    @Override
                    @Timed
                    public Response apply(ContainerRequestContext context) {
                        return runner.stop();
                    }
                });

        return resourceBuilder.build();
    }
}
