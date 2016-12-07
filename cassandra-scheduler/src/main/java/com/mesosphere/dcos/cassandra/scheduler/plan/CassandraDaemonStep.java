package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.DefaultStep;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

public class CassandraDaemonStep extends DefaultStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDaemonStep.class);

    private final CassandraState cassandraState;
    private final PersistentOfferRequirementProvider provider;
    private volatile CassandraMode mode = CassandraMode.UNKNOWN;

    public static boolean isComplete(Protos.TaskStatus status) {
        return isComplete(Optional.of(status));
    }

    public static boolean isComplete(Optional<Protos.TaskStatus> status) {
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

    public static CassandraDaemonStep create(
            final String name,
            final PersistentOfferRequirementProvider provider,
            final CassandraState cassandraState) throws IOException {

        return new CassandraDaemonStep(
                name,
                provider,
                cassandraState);
    }

    public CassandraDaemonStep(
            final String name,
            final PersistentOfferRequirementProvider provider,
            final CassandraState cassandraState) throws IOException {
        super(name, Optional.empty(), Status.PENDING, Collections.emptyList());
        this.cassandraState = cassandraState;
        this.provider = provider;
        if (isComplete(cassandraState.getOrCreateContainer(name))) {
            setStatus(Status.COMPLETE);
        }
    }

    @Override
    public Optional<OfferRequirement> start() {
        LOGGER.info("Starting Step = {}", getName());
        try {
            CassandraContainer container = cassandraState.getOrCreateContainer(getName());

            if (!isPending()) {
                LOGGER.warn("Step {} is not pending. start() should not be called.", getName());
                return Optional.empty();
            }

            if (isComplete(container)) {
                LOGGER.info("Step {} - Task complete: id = {}",
                        getName(),
                        container.getId());
                setStatus(Status.COMPLETE);
                return Optional.empty();
            } else if (StringUtils.isBlank(container.getAgentId())) {
                LOGGER.info("Step {} - Launching new container : id = {}",
                        getName(),
                        container.getId());
                return provider.getNewOfferRequirement(container);
            } else if (needsConfigUpdate(container.getDaemonTask())) {
                LOGGER.info("Step {} - Task requires config update: id = {}",
                        getName(),
                        container.getId());
                return reconfigureTask(container.getDaemonTask());
            } else if (container.isTerminated() || container.isLaunching()) {
                LOGGER.info("Step {} - Replacing container : id = {}",
                        getName(),
                        container.getId());
                return replaceTask(container.getDaemonTask());
            } else {
                return Optional.empty();
            }
        } catch (IOException ex) {
            LOGGER.error(String.format("Step %s - Failed to get or create a container", getName()), ex);
            return Optional.empty();
        }
    }

    @Override
    public void update(Protos.TaskStatus status) {
        try {
            final String taskName = org.apache.mesos.offer.TaskUtils.toTaskName(status.getTaskId());
            if (!getName().equals(taskName)) {
                LOGGER.debug("TaskStatus was meant for step: {} and doesn't affect step {}. Status: {}",
                        taskName, getName(), TextFormat.shortDebugString(status));
                return;
            }
            if (isPending()) {
                LOGGER.info("Ignoring TaskStatus (Step {} is Pending): {}", getName(), TextFormat.shortDebugString(status));
                return;
            }
            if (status.getReason().equals(Protos.TaskStatus.Reason.REASON_RECONCILIATION)) {
                LOGGER.info("Ignoring TaskStatus (Reason is RECONCILIATION): {}", TextFormat.shortDebugString(status));
                return;
            }
            if (status.hasData()) {
                final CassandraData cassandraData = CassandraData.parse(status.getData());
                mode = cassandraData.getMode();
                LOGGER.info("{} Step: {} received status: {} with mode: {}",
                        getStatus(), getName(), TextFormat.shortDebugString(status), cassandraData.getMode());
            } else {
                LOGGER.info("{} Step: {} received status: {}",
                        getStatus(), getName(), TextFormat.shortDebugString(status));
            }
            if (isComplete(status)) {
                setStatus(Status.COMPLETE);
                LOGGER.info("Updating step: {} with: {}", getName(), Status.COMPLETE);
            } else if (CassandraTaskStatus.isTerminated(status.getState())) {
                setStatus(Status.PENDING);
                LOGGER.info("Updating step: {} with: {}", getName(), Status.PENDING);
            } else {
                LOGGER.info("TaskStatus doesn't affect step: {}", TextFormat.shortDebugString(status));
            }
        } catch (Exception ex) {
            LOGGER.error(String.format("Step %s - Failed to update status: %s", getName(), status), ex);
        }
    }

    @Override
    public String getMessage() {
        return "Deploying Cassandra node " + getName() + " mode: " + mode.name();
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", getName(), getStatus());
    }
}
