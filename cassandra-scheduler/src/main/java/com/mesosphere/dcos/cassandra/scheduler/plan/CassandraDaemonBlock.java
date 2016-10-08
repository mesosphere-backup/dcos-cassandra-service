package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
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

public class CassandraDaemonBlock extends DefaultObservable implements Block {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraDaemonBlock.class);

    private final UUID id = UUID.randomUUID();
    private final CassandraState cassandraState;
    private final PersistentOfferRequirementProvider provider;
    private final String name;
    private volatile Status status = Status.PENDING;
    private volatile CassandraMode mode = CassandraMode.UNKNOWN;

    private CassandraContainer getTask() throws PersistenceException, ConfigStoreException {
        return cassandraState.getOrCreateContainer(name);
    }

    public static boolean isComplete(Protos.TaskStatus status) throws IOException {
        return isComplete(Optional.of(status));
    }

    public static boolean isComplete(Optional<Protos.TaskStatus> status) throws IOException {
        if (!status.isPresent()) {
            return false;
        }

        final boolean isRunning = Protos.TaskState.TASK_RUNNING.equals(status.get().getState());

        if (!isRunning) {
            return false;
        }

        if (status.get().hasData()) {
            final CassandraData data = CassandraData.parse(status.get().getData());
            final CassandraMode mode = data.getMode();
            String hostName = data.getHostname();
            final boolean isModeNormal = CassandraMode.NORMAL.equals(mode);
            LOGGER.info("isRunning: {} isModeNormal: {} hostName: {}", isRunning, isModeNormal, hostName);
            return isModeNormal;
        }

        LOGGER.info("Status does not yet indicate mode NORMAL");
        return false;
    }

    private boolean isComplete(final CassandraContainer container) throws IOException {
        final String name = container.getDaemonTask().getName();
        final Optional<Protos.TaskStatus> storedStatus = cassandraState.getStateStore().fetchStatus(name);
        if (storedStatus.isPresent()) {
            final boolean needsConfigUpdate = needsConfigUpdate(container.getDaemonTask());
            return isComplete(storedStatus.get()) && !needsConfigUpdate;
        } else {
            return false;
        }
    }

    private boolean needsConfigUpdate(final CassandraDaemonTask task) throws ConfigStoreException {
        return cassandraState.needsConfigUpdate(task);
    }

    private Optional<OfferRequirement> reconfigureTask(final CassandraDaemonTask task)
            throws ConfigStoreException, PersistenceException {
        final CassandraTemplateTask templateTask = cassandraState
                .getOrCreateTemplateTask(CassandraTemplateTask
                .toTemplateTaskName(task.getName()), task);
        return provider.getReplacementOfferRequirement(
                cassandraState.createCassandraContainer(cassandraState.reconfigureDaemon(task), templateTask));
    }

    private Optional<OfferRequirement> replaceTask(final CassandraDaemonTask task) throws PersistenceException {
        String templateTaskName = CassandraTemplateTask.toTemplateTaskName(task.getName());
        CassandraTemplateTask templateTask = cassandraState.getOrCreateTemplateTask(templateTaskName, task);
        return provider.getReplacementOfferRequirement(
                cassandraState.createCassandraContainer(cassandraState.replaceDaemon(task), templateTask));
    }

    public static CassandraDaemonBlock create(
            final String name,
            final PersistentOfferRequirementProvider provider,
            final CassandraState cassandraState) throws IOException {

        return new CassandraDaemonBlock(
                name,
                provider,
                cassandraState);
    }

    public CassandraDaemonBlock(
            final String name,
            final PersistentOfferRequirementProvider provider,
            final CassandraState cassandraState) throws IOException {
        this.cassandraState = cassandraState;
        this.name = name;
        this.provider = provider;

        CassandraContainer container = cassandraState.getOrCreateContainer(name);
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
                return reconfigureTask(container.getDaemonTask());
            } else if (container.isTerminated() || container.isLaunching()) {
                LOGGER.info("Block {} - Replacing container : id = {}",
                        getName(),
                        container.getId());
                return replaceTask(container.getDaemonTask());
            } else {
                return Optional.empty();
            }
        } catch (IOException ex) {
            LOGGER.error(String.format("Block %s - Failed to get or create a " +
                    "container", getName()), ex);
            return Optional.empty();
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
                mode = cassandraData.getMode();
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
        return "Deploying Cassandra node " + getName() + " mode: " + mode.name();
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
        Status oldStatus = status;
        status = newStatus;
        if (!oldStatus.equals(status)) {
            notifyObservers();
        }
    }
}
