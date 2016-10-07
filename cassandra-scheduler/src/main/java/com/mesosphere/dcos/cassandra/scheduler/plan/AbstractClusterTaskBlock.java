package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.DefaultObservable;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

public abstract class AbstractClusterTaskBlock<C extends ClusterTaskContext> extends DefaultObservable implements Block {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AbstractClusterTaskBlock.class);

    private final UUID id = UUID.randomUUID();
    private final String daemon;
    private volatile Status status;
    private final C context;
    private final CassandraOfferRequirementProvider provider;
    protected final CassandraState cassandraState;

    protected abstract Optional<CassandraTask> getOrCreateTask(C context)
            throws PersistenceException;

    protected OfferRequirement getOfferRequirement(CassandraTask task) {
        if (Protos.TaskState.TASK_FINISHED.equals(
                task.getState())) {
            // Task is already finished
            LOGGER.info(
                    "Task {} assigned to this block {}, is already in state: {}",
                    task.getId(),
                    id,
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

    @Override
    public Optional<OfferRequirement> start() {
        LOGGER.info("Starting Block: name = {}, id = {}",
                getName(),
                getId());

        try {
            // Is Daemon task running ?
            final Optional<Protos.TaskStatus> lastKnownDaemonStatus = cassandraState.getStateStore().fetchStatus(getDaemon());
            if (!lastKnownDaemonStatus.isPresent()) {
                LOGGER.info("Daemon is not present in StateStore.");
                return Optional.empty();
            }

            if (!CassandraDaemonBlock.isComplete(lastKnownDaemonStatus.get())) {
                LOGGER.info("Daemon block is not complete.");
                return Optional.empty();
            }

            Optional<CassandraTask> task = getOrCreateTask(context);
            if (task.isPresent()) {
                if (isComplete() || isInProgress()) {
                    LOGGER.info("No requirement because block is: ", status);
                    return Optional.empty();
                } else {
                    setStatus(Status.PENDING);
                }
            }

            if (!task.isPresent()) {
                LOGGER.info("Block has no task: name = {}, id = {}",
                        getName(), getId());

                return Optional.empty();
            } else {
                LOGGER.info("Block has task: " + task);
                return Optional.of(getOfferRequirement(task.get()));
            }

        } catch (IOException ex) {
            LOGGER.error(String.format("Block failed to create offer " +
                            "requirement: name = %s, id = %s",
                    getName(),
                    getId()), ex);

            return Optional.empty();
        }
    }

    public AbstractClusterTaskBlock(
            final String daemon,
            final CassandraState cassandraState,
            final CassandraOfferRequirementProvider provider,
            final C context) {
        this.daemon = daemon;
        this.provider = provider;
        this.status = Status.PENDING;
        this.context = context;
        this.cassandraState = cassandraState;
        Optional<CassandraTask> taskOption = cassandraState.get(getName());
        if (taskOption.isPresent()) {
            CassandraTask task = taskOption.get();
            if (Protos.TaskState.TASK_FINISHED.equals(
                    task.getState()
            )) {
                setStatus(Status.COMPLETE);
            }
        }
    }

    public abstract String getName();

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
                    LOGGER.info("Reallocating task {} for block {}",
                            getName(),
                            id);
                    setStatus(Status.PENDING);
                }
            }

        } catch (Exception ex) {
            LOGGER.error(
                    String.format("Exception for task {} in block {}. Block " +
                                    "failed to progress",
                            getName(),
                            id), ex);
        }
    }


    @Override
    public String getMessage() {
        return "Block " + getName() + " status = " + status;
    }

    @Override
    public boolean isPending() {
        return Status.PENDING == this.status;
    }

    @Override
    public boolean isInProgress() {
        return Status.IN_PROGRESS == this.status;
    }

    @Override
    public void updateOfferStatus(Collection<Protos.Offer.Operation> operations) {
        //TODO(nick): Any additional actions to perform when OfferRequirement returned by start()
        //            was accepted or not accepted?
        if (!operations.isEmpty()) {
            setStatus(Status.IN_PROGRESS);
        } else {
            setStatus(Status.PENDING);
        }
    }

    @Override
    public void restart() {
        //TODO(nick): Any additional actions to perform when restarting work?
        setStatus(Status.PENDING);
    }

    @Override
    public void forceComplete() {
        //TODO(nick): Any additional actions to perform when forcing complete?
        setStatus(Status.COMPLETE);
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public boolean isComplete() {
        return Status.COMPLETE == this.status;
    }

    public String getDaemon() {
        return daemon;
    }

    protected void setStatus(Status newStatus) {
        LOGGER.info("{}: changing status from: {} to: {}", getName(), status, newStatus);
        Status oldStatus = status;
        status = newStatus;

        if (oldStatus != status) {
            notifyObservers();
        }
    }
}
