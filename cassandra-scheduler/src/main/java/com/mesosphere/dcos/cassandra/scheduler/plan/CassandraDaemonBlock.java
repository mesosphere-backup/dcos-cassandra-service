package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.apache.mesos.state.StateStoreException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        } else {
            LOGGER.info("Task '{}' is already terminated on host '{}'", getName(), task.getId(), task.getHostname());
        }
    }

    private CassandraContainer getTask() throws PersistenceException, ConfigStoreException {
        return cassandraTasks.getOrCreateContainer(name);
    }

    private boolean isComplete(Protos.TaskStatus status) throws IOException {
        final CassandraData cassandraData = CassandraData.parse(status.getData());
        final boolean isRunning = Protos.TaskState.TASK_RUNNING.equals(status.getState());
        final boolean isModeNormal = CassandraMode.NORMAL.equals(cassandraData.getMode());
        LOGGER.info("isRunning: {} isModeNormal: {}", isRunning, isModeNormal);
        return (isRunning && isModeNormal);
    }

    private boolean isComplete(final CassandraContainer container) throws ConfigStoreException {
        final String name = container.getDaemonTask().getName();
        try {
            final Protos.TaskStatus storedStatus = cassandraTasks.getStateStore().fetchStatus(name);

            if (storedStatus.hasData()) {
                final CassandraData data = CassandraData.parse(storedStatus.getData());
                final CassandraMode mode = data.getMode();
                return (Protos.TaskState.TASK_RUNNING.equals(storedStatus.getState())
                        && CassandraMode.NORMAL.equals(mode) &&
                        !cassandraTasks.needsConfigUpdate(container.getDaemonTask()));
            } else {
                return false;
            }
        } catch (StateStoreException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof KeeperException.NoNodeException) {
                // No stored status, must be incomplete
                return false;
            }
            throw e;
        }
    }

    private boolean needsConfigUpdate(final CassandraDaemonTask task) throws ConfigStoreException {
        return cassandraTasks.needsConfigUpdate(task);
    }

    private OfferRequirement reconfigureTask(final CassandraDaemonTask task) throws ConfigStoreException {
        try {
            final CassandraTemplateTask templateTask = cassandraTasks.getOrCreateTemplateTask(CassandraTemplateTask
                    .toTemplateTaskName(task.getName()), task);
            return provider.getReplacementOfferRequirement(cassandraTasks
                    .createCassandraContainer(cassandraTasks.reconfigureDaemon(task), templateTask));
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
                    cassandraTasks.createCassandraContainer(cassandraTasks.replaceDaemon(task)));
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
            final SchedulerClient client) throws PersistenceException, ConfigStoreException {

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
            final SchedulerClient client) throws PersistenceException, ConfigStoreException {
        this.cassandraTasks = cassandraTasks;
        this.name = name;
        this.provider = provider;
        this.client = client;

        CassandraContainer container = cassandraTasks.getOrCreateContainer(name);
        if (isComplete(container)) {
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

            if (!isPending()) {
                LOGGER.warn("Block {} is not pending. start() should not be called.", getName());
                return null;
            }

            if (isComplete(container)) {
                LOGGER.info("Block {} - Task complete: id = {}",
                        getName(),
                        container.getId());
                setStatus(Status.Complete);
                return null;
            } else if (StringUtils.isBlank(container.getAgentId())) {
                LOGGER.info("Block {} - Launching new container : id = {}",
                        getName(),
                        container.getId());
                return provider.getNewOfferRequirement(container);
            } else if (needsConfigUpdate(container.getDaemonTask())) {
                LOGGER.info("Block {} - Task requires config update: id = {}",
                        getName(),
                        container.getId());
                final String name = container.getDaemonTask().getName();
                final Protos.TaskStatus status = cassandraTasks.getStateStore().fetchStatus(name);
                if (!CassandraDaemonStatus.isTerminated(status.getState())) {
                    terminate(container.getDaemonTask());
                    return null;
                } else {
                    return reconfigureTask(container.getDaemonTask());
                }
            } else if (container.isTerminated() || container.isLaunching()) {
                LOGGER.info("Block {} - Replacing container : id = {}",
                        getName(),
                        container.getId());
                return replaceTask(container.getDaemonTask());
            } else {
                return null;
            }
        } catch (PersistenceException | ConfigStoreException ex) {
            LOGGER.error(String.format("Block %s - Failed to get or create a " +
                    "container", getName()), ex);
            return null;
        }
    }

    @Override
    public void update(Protos.TaskStatus status) {
        try {
            final String taskName = org.apache.mesos.offer.TaskUtils.toTaskName(status.getTaskId());
            if (!getName().equals(taskName)) {
                LOGGER.info("TaskStatus was meant for block: {} and doesn't affect block {}. Status: {}",
                        taskName, getName(), status);
                return;
            }
            if (isPending()) {
                LOGGER.info("Ignoring TaskStatus (Block {} is Pending): {}", getName(), status);
                return;
            }
            if (status.getReason().equals(Protos.TaskStatus.Reason.REASON_RECONCILIATION)) {
                LOGGER.info("Ignoring TaskStatus (Reason is RECONCILIATION): {}", status);
                return;
            }
            final CassandraData cassandraData = CassandraData.parse(status.getData());
            LOGGER.info("{} Block: {} received status: {} with mode: {}",
                    Block.getStatus(this), getName(), status, cassandraData.getMode());
            if (isComplete(status)) {
                setStatus(Status.Complete);
                LOGGER.info("Updating block: {} with: {}", getName(), Status.Complete);
            } else if (CassandraTaskStatus.isTerminated(status.getState())) {
                setStatus(Status.Pending);
                LOGGER.info("Updating block: {} with: {}", getName(), Status.Pending);
            } else {
                LOGGER.info("TaskStatus doesn't affect block: {}", status);
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
            if (!isComplete()) {
                setStatus(Status.Pending);
            }
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
