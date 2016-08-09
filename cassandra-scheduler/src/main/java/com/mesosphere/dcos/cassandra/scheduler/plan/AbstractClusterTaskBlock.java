package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

public abstract class AbstractClusterTaskBlock<C extends ClusterTaskContext> implements Block {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AbstractClusterTaskBlock.class);

    private final UUID id = UUID.randomUUID();
    private final String daemon;
    private volatile Status status;
    private final C context;
    private final CassandraOfferRequirementProvider provider;
    protected final CassandraTasks cassandraTasks;

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
            setStatus(Status.Complete);
            return null;
        } else if (task.getSlaveId().isEmpty()) {
            //we have not yet been assigned a slave id - This means that the
            //the task has never been launched
            setStatus(Status.InProgress);
            return provider.getNewOfferRequirement(task.getTaskInfo());
        } else {
            setStatus(Status.InProgress);
            return provider.getUpdateOfferRequirement(task.getTaskInfo());

        }
    }

    @Override
    public OfferRequirement start() {
        LOGGER.info("Starting Block: name = {}, id = {}",
                getName(),
                getId());

        try {
            Optional<CassandraTask> task = getOrCreateTask(context);
            if (task.isPresent()) {
                update(task.get().getCurrentStatus());
                if (isComplete() || isInProgress()) {
                    return null;
                } else {
                    setStatus(Status.Pending);
                }
            }

            if (!task.isPresent()) {
                LOGGER.info("Block has no task: name = {}, id = {}",
                        getName(), getId());

                return null;
            } else {
                LOGGER.info("Block has task: " + task);
                return getOfferRequirement(task.get());
            }

        } catch (PersistenceException ex) {

            LOGGER.error(String.format("Block failed to create offer " +
                            "requirement: name = %s, id = %s",
                    getName(),
                    getId()), ex);

            return null;
        }
    }

    public AbstractClusterTaskBlock(
            final String daemon,
            final CassandraTasks cassandraTasks,
            final CassandraOfferRequirementProvider provider,
            final C context) {
        this.daemon = daemon;
        this.provider = provider;
        this.status = Status.Pending;
        this.context = context;
        this.cassandraTasks = cassandraTasks;
        Optional<CassandraTask> taskOption = cassandraTasks.get(getName());
        if (taskOption.isPresent()) {
            CassandraTask task = taskOption.get();
            if (Protos.TaskState.TASK_FINISHED.equals(
                    task.getState()
            )) {
                setStatus(Status.Complete);
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
            cassandraTasks.update(status);
            Optional<CassandraTask> taskOption = cassandraTasks.get(getName());

            if (taskOption.isPresent()) {
                CassandraTask task = taskOption.get();
                if (Protos.TaskState.TASK_FINISHED.equals(task.getState())) {
                    setStatus(Status.Complete);
                } else if (Protos.TaskState.TASK_RUNNING.equals(task.getState())) {
                    setStatus(Status.InProgress);
                } else if (task.isTerminated()) {
                    //need to progress with a new task
                    cassandraTasks.remove(getName());
                    LOGGER.info("Reallocating task {} for block {}",
                            getName(),
                            id);
                    setStatus(Status.Pending);
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
        return Status.Pending == this.status;
    }

    @Override
    public boolean isInProgress() {
        return Status.InProgress == this.status;
    }

    @Override
    public void updateOfferStatus(boolean accepted) {
        //TODO(nick): Any additional actions to perform when OfferRequirement returned by start()
        //            was accepted or not accepted?
        if (accepted) {
            setStatus(Status.InProgress);
        } else {
            setStatus(Status.Pending);
        }
    }

    @Override
    public void restart() {
        //TODO(nick): Any additional actions to perform when restarting work?
        setStatus(Status.Pending);
    }

    @Override
    public void forceComplete() {
        //TODO(nick): Any additional actions to perform when forcing complete?
        setStatus(Status.Complete);
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public boolean isComplete() {
        return Status.Complete == this.status;
    }

    public String getDaemon() {
        return daemon;
    }

    protected void setStatus(Status newStatus) {
        LOGGER.info("{}: changing status from: {} to: {}", getName(), status, newStatus);
        status = newStatus;
    }
}
