package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.lang3.StringUtils;
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
    private final PersistentOfferRequirementProvider provider;
    private final SchedulerClient client;
    private final String name;
    private boolean terminated = false;
    private volatile Status status = Status.Pending;

    private void terminate(final CassandraDaemonTask task) {
        LOGGER.info("Block '{}' terminating task '{}' on host '{}'", getName(), task.getId(), task.getHostname());
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

    private CassandraContainer getTask() throws PersistenceException {
        return cassandraTasks.getOrCreateContainer(name);
    }

    private boolean isComplete(final CassandraContainer container) {
        return (Protos.TaskState.TASK_RUNNING.equals(
                container.getState())
                && CassandraMode.NORMAL.equals(
                container.getMode()) &&
                !cassandraTasks.needsConfigUpdate(container.getDaemonTask()));
    }

    private boolean needsConfigUpdate(final CassandraDaemonTask task) {
        return cassandraTasks.needsConfigUpdate(task);
    }

    private OfferRequirement reconfigureTask(final CassandraDaemonTask task) {

        try {
            return provider.getReplacementOfferRequirement(
                    cassandraTasks.reconfigureDeamon(task).getTaskInfo()
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
                    cassandraTasks.replaceDaemon(task).getTaskInfo()
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
            final PersistentOfferRequirementProvider provider,
            final CassandraTasks cassandraTasks,
            final SchedulerClient client) throws PersistenceException {

        return new CassandraDaemonBlock(
                name,
                provider,
                cassandraTasks,
                client);
    }

    public CassandraDaemonBlock(
            final String name,
            final PersistentOfferRequirementProvider provider,
            final CassandraTasks cassandraTasks,
            final SchedulerClient client) throws PersistenceException {
        this.cassandraTasks = cassandraTasks;
        this.name = name;
        this.provider = provider;
        this.client = client;

        CassandraContainer container = cassandraTasks.getOrCreateContainer(name);
        if (!needsConfigUpdate(container.getDaemonTask()) && isComplete(container)) {
            setStatus(Status.Complete);
        }
    }

    @Override
    public boolean isPending() {
        return Status.Pending.equals(status);
    }

    @Override
    public boolean isInProgress() {
        return Status.InProgress.equals(status);
    }

    @Override
    public boolean isComplete() {
        return Status.Complete.equals(status);
    }

    @Override
    public OfferRequirement start() {
        LOGGER.info("Starting Block = {}", getName());
        final CassandraContainer container;

        try {
            container = getTask();
        } catch (PersistenceException ex) {
            LOGGER.error(String.format("Block %s - Failed to get or create a " +
                    "container", getName()), ex);
            return null;
        }

        if (isComplete(container)) {
            LOGGER.info("Block {} - Task complete : id = {}",
                    getName(),
                    container.getId());
            setStatus(Status.Complete);
            return null;
        } else if (needsConfigUpdate(container.getDaemonTask())) {
            LOGGER.info("Block {} - Task requires config update : id = {}",
                    getName(),
                    container.getId());
            if (!container.isTerminated()) {
                terminate(container.getDaemonTask());
                return null;
            } else {
                return reconfigureTask(container.getDaemonTask());
            }
        } else if (StringUtils.isBlank(container.getAgentId())) {
            LOGGER.info("Block {} - Launching new container : id = {}",
                    getName(),
                    container.getId());
            return provider.getNewOfferRequirement(container);
        } else if (container.isTerminated() || container.isLaunching()) {
            LOGGER.info("Block {} - Replacing container : id = {}",
                    getName(),
                    container.getId());
            return replaceTask(container.getDaemonTask());
        } else {
            return null;
        }
    }

    @Override
    public void update(Protos.TaskStatus status) {

        try {
            cassandraTasks.update(status);
            final CassandraContainer task = getTask();
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
        //TODO(nick): Any additional actions to perform when restarting work? eg terminated=false?
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
    public String getMessage() {
        return "Deploying Cassandra node " + getName();
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "CassandraDaemonBlock{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }

    private void setStatus(Status newStatus) {
        LOGGER.info("{}: changing status from: {} to: {}", getName(), status, newStatus);
        status = newStatus;
    }
}
