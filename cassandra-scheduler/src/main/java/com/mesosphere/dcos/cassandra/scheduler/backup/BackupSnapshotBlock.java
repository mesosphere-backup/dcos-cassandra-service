package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.Subscribe;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BackupSnapshotBlock implements CassandraBlock {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupSnapshotBlock.class);

    public static BackupSnapshotBlock create(
            int id,
            String taskId,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            BackupContext context) {
        return new BackupSnapshotBlock(id, taskId, cassandraTasks, provider, context);
    }

    public static final String PREFIX = "snapshot-";

    private int id;
    private String taskId;
    private Status status;
    private BackupContext backupContext;
    private CassandraTasks cassandraTasks;
    private CassandraOfferRequirementProvider provider;

    public BackupSnapshotBlock(int id,
                               String taskId,
                               CassandraTasks cassandraTasks,
                               CassandraOfferRequirementProvider provider,
                               BackupContext context) {
        this.id = id;
        this.taskId = taskId;
        this.provider = provider;
        this.status = Status.Pending;
        this.backupContext = context;
        this.cassandraTasks = cassandraTasks;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void setStatus(Status newStatus) {
        LOGGER.info("{}: changing status from: {} to: {}", getName(), status, newStatus);
        status = newStatus;
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
    public OfferRequirement start() {
        LOGGER.info("Starting block: {}", getName());
        final BackupSnapshotTask task = cassandraTasks.getBackupSnapshotTasks().get(taskId);

        // This will work better once reconcilation is implemented
        if (Protos.TaskState.TASK_FINISHED.equals(task.getStatus().getState())) {
            // Task is already finished
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
                LOGGER.debug("Irrelevant status id = {}, task = {}, status = {}",
                        id, taskId, status);
                return;
            } else {
                cassandraTasks.update(status);

                BackupSnapshotTask task = cassandraTasks.getBackupSnapshotTasks()
                        .get(taskId);

                if (task != null && Protos.TaskState.TASK_FINISHED == task.getStatus().getState()) {
                    setStatus(Status.Complete);
                } else if (TaskUtils.isTerminated(status.getState())) {
                    //need to progress with a new task
                    cassandraTasks.remove(status.getTaskId().getValue());
                    taskId = cassandraTasks.createBackupSnapshotTask(this.id, this.backupContext).getId();
                    LOGGER.info("Reallocating task {} for block {}",
                            taskId,
                            id);
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
        return PREFIX + id;
    }

    @Override
    public String getName() {
        return getNodeName();
    }

    @Override
    public boolean isComplete() {
        return Status.Complete == this.status;
    }

    @Override
    public String getTaskId() {
        return taskId;
    }
}
