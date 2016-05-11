package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CassandraDaemonBlock implements Block {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraDaemonBlock.class);

    private final UUID id = UUID.randomUUID();
    private final CassandraTasks cassandraTasks;
    private final CassandraOfferRequirementProvider provider;
    private final SchedulerClient client;
    private final String name;
    private boolean terminated = false;
    private volatile Status status = Status.Pending;

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

    private void updateConfig(final CassandraDaemonTask task) {
        try {
            CassandraDaemonTask updated =
                    cassandraTasks.reconfigureDeamon(task);
            LOGGER.info("Block {} reconfiguring task : id = {}",
                    getName(),
                    updated.getId());
        } catch (PersistenceException ex) {
            LOGGER.error(String.format("Block %s - Failed to get or " +
                    "reconfigure task", getName()), ex);
        }
    }

    private CassandraDaemonTask getTask() throws PersistenceException {
        return cassandraTasks.getOrCreateDaemon(name);
    }

    private boolean isComplete(final CassandraDaemonTask task) {
        return (Protos.TaskState.TASK_RUNNING.equals(
                task.getStatus().getState())
                && CassandraMode.NORMAL.equals(
                task.getStatus().getMode()) &&
                !cassandraTasks.needsConfigUpdate(task));
    }

    private boolean needsConfigUpdate(final CassandraDaemonTask task) {
        return cassandraTasks.needsConfigUpdate(task);
    }

    private OfferRequirement reconfigureTask(final CassandraDaemonTask task) {

        try {
            return provider.getReplacementOfferRequirement(
                    cassandraTasks.reconfigureDeamon(task).toProto()
            );
        } catch (PersistenceException ex) {
            LOGGER.error(
                    String.format("Block %s failed to reconfigure task %s,"),
                    getName(),
                    task,
                    ex);
            return null;
        }
    }

    private OfferRequirement replaceTask(final CassandraDaemonTask task) {
        try {
            return provider.getReplacementOfferRequirement(
                    cassandraTasks.replaceDaemon(task).toProto()
            );
        } catch (PersistenceException ex) {
            LOGGER.error(
                    String.format("Block %s failed to replace task %s,"),
                    getName(),
                    task,
                    ex);
            return null;
        }
    }

    public static CassandraDaemonBlock create(
            final String name,
            final CassandraOfferRequirementProvider provider,
            final CassandraTasks cassandraTasks,
            final SchedulerClient client) {

        return new CassandraDaemonBlock(
                name,
                provider,
                cassandraTasks,
                client);
    }

    public CassandraDaemonBlock(
            final String name,
            final CassandraOfferRequirementProvider provider,
            final CassandraTasks cassandraTasks,
            final SchedulerClient client) {
        this.cassandraTasks = cassandraTasks;
        this.name = name;
        this.provider = provider;
        this.client = client;

        if (cassandraTasks.getDaemons().containsKey(name)) {
            CassandraDaemonTask task = cassandraTasks.getDaemons().get(name);
            if (!needsConfigUpdate(task) && isComplete(task)) {
                status = Status.Complete;
            }
        }
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public boolean isPending() {
        return Status.Pending.equals(getStatus());
    }

    @Override
    public boolean isInProgress() {

        return Status.InProgress.equals(getStatus());
    }

    @Override
    public OfferRequirement start() {
        LOGGER.info("Starting Block = {}", getName());
        final CassandraDaemonTask task;

        try {
            task = getTask();
        } catch (PersistenceException ex) {
            LOGGER.error(String.format("Block %s - Failed to get or create a " +
                    "task", getName()), ex);
            return null;
        }

        if (isComplete(task)) {
            LOGGER.info("Block {} - Task complete : id = {}",
                    getName(),
                    task.getId());
            setStatus(Status.Complete);
            return null;
        } else if (needsConfigUpdate(task)) {
            LOGGER.info("Block {} - Task requires config update : id = {}",
                    getName(),
                    task.getId());
            if (!task.isTerminated()) {
                terminate(task);
                return null;
            } else {
                return reconfigureTask(task);
            }
        } else if (task.getSlaveId().isEmpty()) {
            LOGGER.info("Block {} - Launching new task : id = {}",
                    getName(),
                    task.getId());
            return provider.getNewOfferRequirement(task.toProto());
        } else if (task.isTerminated() || task.isLaunching()) {
            LOGGER.info("Block {} - Replacing task : id = {}",
                    getName(),
                    task.getId());
            return replaceTask(task);
        } else {
            return null;
        }
    }

    @Override
    public void update(Protos.TaskStatus status) {

        try {
            cassandraTasks.update(status);
            final CassandraDaemonTask task = getTask();
            if (isComplete(task)) {
                setStatus(Status.Complete);
            } else if (task.isTerminated()) {
                setStatus(Status.Pending);
            }
        } catch (Exception ex) {
            LOGGER.error(String.format("Block %s - Failed update status " +
                            "task : status = %s", getName(), status),
                    ex);
        }
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public String getMessage() {
        return "Deploying Cassandra node " + getName();
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
        LOGGER.info("Block {} setting status to {}",
                getName(), newStatus);
        status = newStatus;
    }


    @Override
    public String toString() {
        return "CassandraDaemonBlock{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }
}
