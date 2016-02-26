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

public class BackupSnapshotBlock extends AbstractClusterTaskBlock<BackupContext> {
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

    public BackupSnapshotBlock(int id,
                               String taskId,
                               CassandraTasks cassandraTasks,
                               CassandraOfferRequirementProvider provider,
                               BackupContext context) {
        super(id, taskId, cassandraTasks, provider, context);
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
                super.id, super.taskId, status);
        try {
            if (!isRelevantStatus(status)) {
                //ignore what is not my concern
                LOGGER.debug("Irrelevant status id = {}, task = {}, status = {}",
                        super.id, super.taskId, status);
                return;
            } else {
                super.cassandraTasks.update(status);

                BackupSnapshotTask task = super.cassandraTasks.getBackupSnapshotTasks()
                        .get(super.taskId);

                if (task != null && Protos.TaskState.TASK_FINISHED == status.getState()) {
                    setStatus(Status.Complete);
                } else if (TaskUtils.isTerminated(status.getState())) {
                    //need to progress with a new task
                    super.cassandraTasks.remove(status.getTaskId().getValue());
                    super.taskId = cassandraTasks.createBackupSnapshotTask(super.id, super.context).getId();
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

    @Override
    public String getName() {
        return PREFIX + super.id;
    }
}
