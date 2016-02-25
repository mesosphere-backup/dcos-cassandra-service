package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.eventbus.Subscribe;
import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraDaemonBlock implements Block {

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
    private final ExecutorClient client;
    private final String name;
    private volatile Status status;
    private boolean terminated = false;

    private void terminate(final CassandraDaemonTask task) {
        LOGGER.info("Block {} terminating task {}", getName(), task.getId());
        if (!terminated) {
            try {
                if (client.shutdown(task.getHostname(),
                        task.getExecutor().getApiPort()
                ).toCompletableFuture().get()) {
                    LOGGER.info("Block {} terminated task : id = {}",
                            getName(),
                            task.getId());
                    terminated = true;
                } else {
                    LOGGER.warn("Block {} failed to terminate task : id = {}",
                            getName(),
                            task.getId());
                    terminated = false;
                }
            } catch (Throwable t) {
                LOGGER.error(String.format("Block %s  - Error terminating " +
                                "task : id = %s",
                        getName(),
                        task.getId())
                        , t);
                terminated = false;
            }
        }
    }

    public static CassandraDaemonBlock create(
            final int id,
            final String name,
            final CassandraOfferRequirementProvider provider,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {

        return new CassandraDaemonBlock(id, name, provider, cassandraTasks,
                client);
    }

    public CassandraDaemonBlock(
            int id,
            String name,
            final CassandraOfferRequirementProvider provider,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {
        this.status = Status.Pending;
        this.cassandraTasks = cassandraTasks;
        this.name = name;
        this.id = id;
        this.provider = provider;
        this.client = client;
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
        LOGGER.info("Starting Block = {}", getName());
        CassandraDaemonTask task;

        try {
            task = cassandraTasks.getOrCreateDaemon(name);
        } catch (PersistenceException ex) {
            LOGGER.error(String.format("Block {} - Failed to get or create a " +
                    "task", getName()), ex);
            return null;
        }

        if (cassandraTasks.needsConfigUpdate(task)) {
            LOGGER.info("Block {} - Task requires configuration update: id = " +
                            "{}",
                    getName(),
                    task.getId());
            if (!isTerminal(task.getStatus().getState())) {
                terminate(task);
                return null;

            } else {

                try {
                    return provider.getReplacementOfferRequirement(
                            cassandraTasks.reconfigureDeamon(task).toProto());
                } catch (PersistenceException ex) {

                    return null;
                }
            }
        } else if (Protos.TaskState.TASK_RUNNING.equals(task.getStatus()
                .getState())
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
        } else if (isTerminal(task.getStatus().getState())) {
            try {
                setStatus(Status.InProgress);
                return provider.getReplacementOfferRequirement(
                        cassandraTasks.replaceDaemon(task).toProto());

            } catch(PersistenceException ex){
                return null;
            }
        } else {
            setStatus(Status.InProgress);
            return provider.getReplacementOfferRequirement(task.toProto());

        }
    }

    @Subscribe
    @Override
    public void update(Protos.TaskStatus status) {
        LOGGER.debug("Updating status : id = {}, task = {}, status = {}",
                id, name, status);
        if (cassandraTasks.getDaemons().containsKey(name)) {
            CassandraDaemonTask task = cassandraTasks.getDaemons().get(name);

            try {
                if (!status.getTaskId().getValue().equalsIgnoreCase(task
                        .getId())) {
                    //ignore what is not my concern
                    LOGGER.debug(
                            "Irrelevant status id = {}, task = {}, status = {}",
                            id, name, status);
                    return;
                } else {
                    //update the status
                    cassandraTasks.update(status);
                    if (Protos.TaskState.TASK_RUNNING.equals(
                            task.getStatus().getState())
                            && CassandraMode.NORMAL.equals(
                            task.getStatus().getMode()) &&
                            !cassandraTasks.needsConfigUpdate(task)) {
                        setStatus(Status.Complete);
                    }

                }
            } catch (Exception ex) {
                LOGGER.error(
                        String.format(
                                "Exception for task {} in block {}. Block " +
                                        "failed to progress",
                                name,
                                id), ex);
            }
        }

    }


    private boolean isRelevantStatus(Protos.TaskStatus status) {
        return status.getTaskId().equals(cassandraTasks.get(name).get()
                .getId());
    }

    @Override
    public List<Protos.TaskID> getUpdateIds() {
        return null;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
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
    public String toString() {
        return "CassandraDaemonBlock{" +
                "name='" + name + '\'' +
                ", status=" + status +
                ", id=" + id +
                '}';
    }
}
