package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.annotations.VisibleForTesting;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClusterTaskStep.class);

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
        LOGGER.info("Starting Step: name = {}, id = {}",
                getName(),
                getId());

        try {
            // Is Daemon task running ?
            final Optional<Protos.TaskStatus> lastKnownDaemonStatus = cassandraState.getStateStore().fetchStatus(daemon);
            if (!lastKnownDaemonStatus.isPresent()) {
                LOGGER.info("Daemon is not present in StateStore.");
                return Optional.empty();
            }

            if (!CassandraDaemonStep.isComplete(lastKnownDaemonStatus.get())) {
                LOGGER.info("Daemon step is not complete.");
                return Optional.empty();
            }

            Optional<CassandraTask> task = getOrCreateTask();
            if (task.isPresent()) {
                if (isComplete() || isInProgress()) {
                    LOGGER.info("No requirement because step is: ", getStatus());
                    return Optional.empty();
                } else {
                    setStatus(Status.PENDING);
                }
            }

            if (!task.isPresent()) {
                LOGGER.info("Step has no task: name = {}, id = {}",
                        getName(), getId());

                return Optional.empty();
            } else {
                LOGGER.info("Step has task: " + task);
                return Optional.of(getOfferRequirement(task.get()));
            }

        } catch (IOException ex) {
            LOGGER.error(String.format("Step failed to create offer " +
                            "requirement: name = %s, id = %s",
                    getName(),
                    getId()), ex);

            return Optional.empty();
        }
    }

    @Override
    public void update(Protos.TaskStatus status) {
        LOGGER.info("Updating status : id = {}, task = {}, status = {}",
                getId(), getName(), status);

        if (isComplete()) {
            LOGGER.warn("Task is already complete, ignoring status update.");
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
                    LOGGER.info("Reallocating task {} for step {}", getName(), getId());
                    setStatus(Status.PENDING);
                }
            }

        } catch (Exception ex) {
            LOGGER.error(String.format(
                    "Exception for task {} in step {}. Step failed to progress", getName(), getId()), ex);
        }
    }

    @VisibleForTesting
    public String getDaemon() {
        return daemon;
    }

    private OfferRequirement getOfferRequirement(CassandraTask task) {
        if (Protos.TaskState.TASK_FINISHED.equals(task.getState())) {
            // Task is already finished
            LOGGER.info(
                    "Task {} assigned to this step {}, is already in state: {}",
                    task.getId(),
                    getId(),
                    task.getState());
            setStatus(Status.COMPLETE);
            return null;
        } else if (task.getSlaveId().isEmpty()) {
            //we have not yet been assigned a slave id - This means that the
            //the task has never been launched
            setStatus(Status.IN_PROGRESS);
            return provider.getNewOfferRequirement(task.getType().name(), task.getTaskInfo());
        } else {
            setStatus(Status.IN_PROGRESS);
            return provider.getUpdateOfferRequirement(task.getType().name(), task.getTaskInfo());

        }
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
