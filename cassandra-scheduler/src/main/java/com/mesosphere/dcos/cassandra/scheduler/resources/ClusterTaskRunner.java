package com.mesosphere.dcos.cassandra.scheduler.resources;

import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskRequest;

/**
 * Common code for starting/stopping Cleanup, Repair, Backup, and Restore tasks.
 */
class ClusterTaskRunner<R extends ClusterTaskRequest, C extends ClusterTaskContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTaskRunner.class);

    private final ClusterTaskManager<R, C> manager;
    private final String taskName;

    /**
     * @param manager task manager which will be driven
     * @param taskName user/log-visible name describing the task
     */
    ClusterTaskRunner(ClusterTaskManager<R, C> manager, String taskName) {
        this.manager = manager;
        this.taskName = taskName;
    }

    Response start(R request) {
        LOGGER.info("Processing start {} request: {}", taskName, request);
        try {
            if (request == null || !request.isValid()) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            } else if (!manager.isInProgress()) {
                manager.start(request);
                LOGGER.info("{} started: request = {}", taskName, request);
                return Response.accepted().build();
            } else {
                // Send error back
                LOGGER.warn("{} already in progress: request = {}", taskName, request);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(ErrorResponse.fromString(String.format(
                                "%s already in progress", taskName)))
                        .build();
            }
        } catch (Throwable t) {
            LOGGER.error(String.format("Error starting %s: request = %s", taskName, request), t);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(ErrorResponse.fromThrowable(t))
                    .build();
        }
    }

    Response stop() {
        LOGGER.info("Processing stop {} request", taskName);
        try {
            if (manager.isInProgress()) {
                manager.stop();
                LOGGER.info("{} stopped", taskName);
                return Response.accepted().build();
            } else {
                // Send error back
                LOGGER.warn("{} already not running", taskName);
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ErrorResponse.fromString(String.format(
                            "%s already not running.", taskName)))
                    .build();
            }
        } catch (Throwable t) {
            LOGGER.error(String.format("Error stopping %s", taskName), t);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(ErrorResponse.fromThrowable(t))
                    .build();
        }
    }
}
