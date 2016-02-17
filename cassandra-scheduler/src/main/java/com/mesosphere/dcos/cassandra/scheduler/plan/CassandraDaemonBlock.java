package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.eventbus.Subscribe;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraDaemonBlock implements CassandraBlock {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraDaemonBlock.class);

    private static final boolean isTerminal(Protos.TaskState state) {

        switch (state) {
            case TASK_STARTING:
                return false;
            case TASK_STAGING:
                return false;
            case TASK_RUNNING:
                return false;
            default:
                return true;
        }
    }

    private final int id;
    private final CassandraTasks cassandraTasks;
    private final CassandraOfferRequirementProvider provider;
    private String taskId;
    private Status status;

    public static CassandraDaemonBlock create(
            int id,
            String taskId,
            CassandraOfferRequirementProvider provider,
            CassandraTasks cassandraTasks) {

        return new CassandraDaemonBlock(id, taskId, provider, cassandraTasks);
    }

    public CassandraDaemonBlock(
            int id,
            String taskId,
            CassandraOfferRequirementProvider provider,
            CassandraTasks cassandraTasks) {
        this.status = Status.Pending;
        this.cassandraTasks = cassandraTasks;
        this.taskId = taskId;
        this.id = id;
        this.provider = provider;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public boolean isPending() {
        return Status.Pending == status;
    }

    @Override
    public boolean isInProgress() {
        return Status.InProgress == status;
    }

    @Override
    public OfferRequirement start() {
        LOGGER.info("Starting block: {}", getName());

        CassandraDaemonTask task = cassandraTasks.getDaemons().get(taskId);

        // This will work better once reconcilation is implemented
        if (Protos.TaskState.TASK_RUNNING.equals(task.getStatus().getState())
                && CassandraMode.NORMAL.equals(task.getStatus().getMode())) {
            //We are running and normal complete this block
            LOGGER.info(
                    "Task {} assigned to this block {}, is already in state: {}",
                    task.getId(),
                    id,
                    task.getStatus().getState());
            setStatus(Status.Complete);
            return null;
        } else if (task.getSlaveId().isEmpty()) {
            //we have not yet been assigned a slave id - This means that the
            //the task has never been launched
            setStatus(Status.InProgress);
            return provider.getNewOfferRequirement(task.toProto());
        } else {
            setStatus(Status.InProgress);
            return provider.getUpdateOfferRequirement(task.toProto());

        }
    }

    @Subscribe
    @Override
    public void update(Protos.TaskStatus status) {
        LOGGER.debug("Updating status : id = {}, task = {}, status = {}",
                id, taskId, status);
        try {
            if (!isRelevantStatus(status)) {
                //ignore what is not my concern
                LOGGER.debug(
                        "Irrelevant status id = {}, task = {}, status = {}",
                        id, taskId, status);
                return;
            } else if (isTerminal(status.getState())) {
                //need to progress with a new task
                cassandraTasks.remove(status.getTaskId().getValue());
                taskId = cassandraTasks.createDaemon().getId();
                LOGGER.info("Reallocating task {} for block {}",
                        taskId,
                        id);
            } else {
                //update the status
                cassandraTasks.update(status);
                CassandraDaemonTask task = cassandraTasks.getDaemons().get(
                        taskId);
                if (Protos.TaskState.TASK_RUNNING.equals(
                        task.getStatus().getState())
                        && CassandraMode.NORMAL.equals(
                        task.getStatus().getMode())) {
                    setStatus(Status.Complete);
                }

            }
        } catch (Exception ex) {
            LOGGER.error(
                    String.format("Exception for task {} in block {}. Block " +
                                    "failed to progress",
                            taskId,
                            id), ex);
        }

    }


    private boolean isRelevantStatus(Protos.TaskStatus status) {
        return taskId.equals(status.getTaskId().getValue());
    }

    @Override
    public List<Protos.TaskID> getUpdateIds() {
        return null;
    }

    @Override
    public int getId() {
        return id;
    }

    public String getNodeName() {
        return "node-" + id;
    }

    @Override
    public String getName() {
        return getNodeName();
    }

    @Override
    public boolean isComplete() {
        return Status.Complete.equals(status);
    }

    @Override
    public void setStatus(Status newStatus) {
        Status oldStatus = status;
        status = newStatus;
        LOGGER.info(getName() +
                ": changing status from: "
                + oldStatus + " to: " + status);
    }

    @Override
    public String getTaskId() {
        return taskId;
    }
}
