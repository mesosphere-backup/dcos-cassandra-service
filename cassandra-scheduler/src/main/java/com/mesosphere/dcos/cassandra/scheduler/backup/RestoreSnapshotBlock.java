package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.Subscribe;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RestoreSnapshotBlock extends AbstractClusterTaskBlock<RestoreContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSnapshotBlock.class);

    public static RestoreSnapshotBlock create(
            int id,
            String taskId,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            RestoreContext context) {
        return new RestoreSnapshotBlock(id, taskId, cassandraTasks, provider, context);
    }

    public static final String PREFIX = "restore-";

    public RestoreSnapshotBlock(int id,
                                String taskId,
                                CassandraTasks cassandraTasks,
                                CassandraOfferRequirementProvider provider,
                                RestoreContext context) {
        super(id, taskId, cassandraTasks, provider, context);
    }

    @Override
    public OfferRequirement start() {
        LOGGER.info("Starting block: {}", getName());
        final RestoreSnapshotTask task = cassandraTasks.getRestoreSnapshotTasks().get(taskId);

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
        LOGGER.info("Updating status: id = {}, task = {}, status = {}",
                id, taskId, status);
        try {
            if (!isRelevantStatus(status)) {
                //ignore what is not my concern
                LOGGER.info("Irrelevant status id = {}, task = {}, status = {}",
                        id, taskId, status);
                return;
            } else {
                cassandraTasks.update(status);

                RestoreSnapshotTask task = cassandraTasks.getRestoreSnapshotTasks()
                        .get(taskId);

                if (task != null && Protos.TaskState.TASK_FINISHED == status.getState()) {
                    setStatus(Status.Complete);
                } else if (TaskUtils.isTerminated(status.getState())) {
                    //need to progress with a new task
                    cassandraTasks.remove(status.getTaskId().getValue());
                    taskId = cassandraTasks.createRestoreSnapshotTask(this.id, this.context).getId();
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
        return PREFIX + id;
    }
}
