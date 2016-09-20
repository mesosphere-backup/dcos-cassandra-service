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
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class CassandraDaemonBlock implements Block {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraDaemonBlock.class);

    private final UUID id = UUID.randomUUID();
    private final CassandraTasks cassandraTasks;
    private final PersistentOfferRequirementProvider provider;
    private final SchedulerClient client;
    private final String name;
    private boolean terminated = false;
    private volatile Status status = Status.PENDING;

    private void terminate(final CassandraDaemonTask task) {
        LOGGER.info("Block '{}' terminating task '{}' on host '{}'", getName(), task.getId(), task.getHostname());
        if (!terminated) {
            try {
                String hostName = task.getHostname();
                int apiPort = task.getExecutor().getApiPort();
                LOGGER.info("Client: " + client);
                CompletionStage<Boolean> booleanCompletableFuture = client.shutdown(hostName, apiPort);
                Boolean isShutdown = booleanCompletableFuture.toCompletableFuture().get();

                if (isShutdown) {
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

    public static boolean isComplete(Protos.TaskStatus status) throws IOException {
        return isComplete(Optional.of(status));
    }

    public static boolean isComplete(Optional<Protos.TaskStatus> status) throws IOException {
        if (!status.isPresent()) {
            return false;
        }

        final boolean isRunning = Protos.TaskState.TASK_RUNNING.equals(status.get().getState());

        if (status.get().hasData()) {
            final CassandraData data = CassandraData.parse(status.get().getData());
            final CassandraMode mode = data.getMode();
            final boolean isModeNormal = CassandraMode.NORMAL.equals(mode);
            LOGGER.info("isRunning: {} isModeNormal: {}", isRunning, isModeNormal);
            return (isRunning) && isModeNormal;
        } else {
            // Handle reconcile messages
            return isRunning;
        }
    }

    private boolean isComplete(final CassandraContainer container) throws IOException {
        final String name = container.getDaemonTask().getName();
        try {
            final Optional<Protos.TaskStatus> taskStatusOptional = cassandraTasks.getStateStore().fetchStatus(name);
            final boolean needsConfigUpdate = cassandraTasks.needsConfigUpdate(container.getDaemonTask());
            return isComplete(taskStatusOptional) && !needsConfigUpdate;
        } catch (StateStoreException | IOException e) {
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

    private Optional<OfferRequirement> reconfigureTask(final CassandraDaemonTask task) throws ConfigStoreException {
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
            return Optional.empty();
        }
    }

    private Optional<OfferRequirement> replaceTask(final CassandraDaemonTask task) {
        try {
            String templateTaskName = CassandraTemplateTask.toTemplateTaskName(task.getName());
            CassandraTemplateTask templateTask = cassandraTasks.getOrCreateTemplateTask(templateTaskName, task);
            return provider.getReplacementOfferRequirement(
                    cassandraTasks.createCassandraContainer(cassandraTasks.replaceDaemon(task), templateTask));
        } catch (PersistenceException ex) {
            LOGGER.error(
                    String.format("Block %s failed to replace task %s,"),
                    getName(),
                    task,
                    ex);
            return Optional.empty();
        }
    }

    public static CassandraDaemonBlock create(
            final String name,
            final PersistentOfferRequirementProvider provider,
            final CassandraTasks cassandraTasks,
            final SchedulerClient client) throws PersistenceException, IOException {

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
            final SchedulerClient client) throws PersistenceException, IOException {
        this.cassandraTasks = cassandraTasks;
        this.name = name;
        this.provider = provider;
        this.client = client;

        CassandraContainer container = cassandraTasks.getOrCreateContainer(name);
        if (isComplete(container)) {
            setStatus(Status.COMPLETE);
        }
    }

    @Override
    public boolean isPending() {
        return Status.PENDING.equals(status);
    }

    @Override
    public boolean isInProgress() {
        return Status.IN_PROGRESS.equals(status);
    }

    @Override
    public boolean isComplete() {
        return Status.COMPLETE.equals(status);
    }

    @Override
    public Optional<OfferRequirement> start() {
        LOGGER.info("Starting Block = {}", getName());
        final CassandraContainer container;

        try {
            container = getTask();

            if (!isPending()) {
                LOGGER.warn("Block {} is not pending. start() should not be called.", getName());
                return Optional.empty();
            }

            if (isComplete(container)) {
                LOGGER.info("Block {} - Task complete: id = {}",
                        getName(),
                        container.getId());
                setStatus(Status.COMPLETE);
                return Optional.empty();
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
                final Protos.TaskStatus status = cassandraTasks.getStateStore().fetchStatus(name).get();
                if (!CassandraDaemonStatus.isTerminated(status.getState())) {
                    terminate(container.getDaemonTask());
                    return Optional.empty();
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
        } catch (IOException ex) {
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
            if (status.hasData()) {
                final CassandraData cassandraData = CassandraData.parse(status.getData());
                LOGGER.info("{} Block: {} received status: {} with mode: {}",
                        Block.getStatus(this), getName(), status, cassandraData.getMode());
            } else {
                LOGGER.info("{} Block: {} received status: {}",
                        Block.getStatus(this), getName(), status);
            }
            if (isComplete(status)) {
                setStatus(Status.COMPLETE);
                LOGGER.info("Updating block: {} with: {}", getName(), Status.COMPLETE);
            } else if (CassandraTaskStatus.isTerminated(status.getState())) {
                setStatus(Status.PENDING);
                LOGGER.info("Updating block: {} with: {}", getName(), Status.PENDING);
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
    public void updateOfferStatus(Collection<Protos.Offer.Operation> operations) {
        //TODO(nick): Any additional actions to perform when OfferRequirement returned by start()
        //            was accepted or not accepted?
        if (!operations.isEmpty()) {
            setStatus(Status.IN_PROGRESS);
        } else {
            if (!isComplete()) {
                setStatus(Status.PENDING);
            }
        }
    }

    @Override
    public void restart() {
        //TODO(nick): Any additional actions to perform when restarting work? eg terminated=false?
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
