package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.DefaultStep;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

public abstract class AbstractClusterTaskStep extends DefaultStep {
    // Non-static to support logging child class:
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final String daemon;
    private final CassandraOfferRequirementProvider provider;
    protected final CassandraState cassandraState;

    public AbstractClusterTaskStep(
            final String daemon,
            final String name,
            final CassandraState cassandraState,
            final CassandraOfferRequirementProvider provider) {
        super(name, Optional.empty(), getInitialStatus(name, cassandraState), Collections.emptyList());
        this.daemon = daemon;
        this.provider = provider;
        this.cassandraState = cassandraState;
    }

    protected abstract Optional<CassandraTask> getOrCreateTask() throws PersistenceException;

    @Override
    public Optional<OfferRequirement> start() {
        logger.info("Starting Step: name = {}, id = {}", getName(), getId());

        try {
            // Is Daemon task running ?
            final Optional<Protos.TaskStatus> lastKnownDaemonStatus = cassandraState.getStateStore().fetchStatus(daemon);
            if (!lastKnownDaemonStatus.isPresent()) {
                logger.info("Daemon is not present in StateStore.");
                return Optional.empty();
            }

            if (!CassandraDaemonStep.isComplete(lastKnownDaemonStatus.get())) {
                logger.info("Daemon step is not complete.");
                return Optional.empty();
            }

            Optional<CassandraTask> taskOptional = getOrCreateTask();
            if (taskOptional.isPresent()) {
                CassandraTask task = taskOptional.get();
                logger.info("Step has task: " + task);
                if (isComplete() || isInProgress()) {
                    logger.info("No requirement because step is: ", getStatus());
                    return Optional.empty();
                }
                if (Protos.TaskState.TASK_FINISHED.equals(task.getState())) {
                    // Task is already finished
                    logger.info("Task {} assigned to this step {}, is already in state: {}",
                            task.getId(), getId(), task.getState());
                    setStatus(Status.COMPLETE);
                    return Optional.empty();
                } else if (task.getSlaveId().isEmpty()) {
                    // we have not yet been assigned a slave id - This means that the task has never been launched
                    return Optional.of(provider.getNewOfferRequirement(task.getType().name(), task.getTaskInfo()));
                } else {
                    return Optional.of(provider.getUpdateOfferRequirement(task.getType().name(), task.getTaskInfo()));
                }
            } else {
                logger.info("Step has no task: name = {}, id = {}", getName(), getId());
                return Optional.empty();
            }

        } catch (IOException ex) {
            logger.error(String.format(
                    "Step failed to create offer requirement: name = %s, id = %s",
                    getName(), getId()), ex);

            return Optional.empty();
        }
    }

    @Override
    public void update(Protos.TaskStatus status) {
        logger.info("Updating status: id = {}, task = {}, status = {}",
                getId(), getName(), TextFormat.shortDebugString(status));

        if (isComplete()) {
            logger.warn("Task is already complete, ignoring status update.");
            return;
        }

        try {
            cassandraState.update(status);
            Optional<CassandraTask> taskOption = cassandraState.get(getName());

            if (taskOption.isPresent()) {
                CassandraTask task = taskOption.get();
                if (Protos.TaskState.TASK_FINISHED.equals(task.getState())) {
                    setStatus(Status.COMPLETE);
                } else if (Protos.TaskState.TASK_RUNNING.equals(task.getState())) {
                    setStatus(Status.IN_PROGRESS);
                } else if (task.isTerminated()) {
                    //need to progress with a new task
                    cassandraState.remove(getName());
                    logger.info("Reallocating task {} for step {}", getName(), getId());
                    setStatus(Status.PENDING);
                }
            }

        } catch (Exception ex) {
            logger.error(String.format(
                    "Exception for task {} in step {}. Step failed to progress", getName(), getId()), ex);
        }
    }

    @VisibleForTesting
    public String getDaemon() {
        return daemon;
    }

    private static Status getInitialStatus(String name, CassandraState cassandraState) {
        Optional<CassandraTask> taskOption = cassandraState.get(name);
        if (taskOption.isPresent()) {
            CassandraTask task = taskOption.get();
            if (Protos.TaskState.TASK_FINISHED.equals(task.getState())) {
                return Status.COMPLETE;
            }
        }
        return Status.PENDING;
    }
}
