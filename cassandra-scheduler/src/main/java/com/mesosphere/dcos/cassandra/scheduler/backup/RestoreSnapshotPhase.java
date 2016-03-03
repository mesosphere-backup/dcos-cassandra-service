package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * During download snapshot phase, snapshotted data will be downloaded to all the cassandra node from
 * external location.
 */
public class RestoreSnapshotPhase extends AbstractClusterTaskPhase<RestoreSnapshotBlock, RestoreContext> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RestoreSnapshotPhase.class);

    public RestoreSnapshotPhase(
            RestoreContext context,
            int servers,
            CassandraTasks cassandraTasks,
            EventBus eventBus,
            ClusterTaskOfferRequirementProvider provider,
            int id) {
        super(context, servers, cassandraTasks, eventBus, provider, id);
    }

    protected List<RestoreSnapshotBlock> createBlocks() {
        final List<RestoreSnapshotBlock> newBlocks = new ArrayList<>(super.servers);
        final List<String> createdBlocks =
                new ArrayList<>(cassandraTasks.getRestoreSnapshotTasks().keySet());

        try {
            for (int i = 0; i < servers; i++) {
                String taskId = null;
                // Do we have an existing id ?
                if (i < createdBlocks.size()) {
                    final Optional<CassandraTask> cassandraTask = cassandraTasks.get(createdBlocks.get(i));
                    if (cassandraTask.isPresent()) {
                        taskId = cassandraTask.get().getId();
                    }
                }

                // If not, create a new one!
                if (StringUtils.isBlank(taskId)) {
                    taskId = cassandraTasks.createRestoreSnapshotTask(i, context).getId();
                }

                final RestoreSnapshotBlock block = RestoreSnapshotBlock.create(i, taskId,
                        cassandraTasks, provider, context);
                newBlocks.add(block);
                eventBus.register(block);
            }
        } catch (Throwable throwable) {
            String message = "Failed to create RestoreSnapshotPhase this is a" +
                    " fatal exception and the program will now exit. Please " +
                    "verify your scheduler configuration and attempt to " +
                    "relaunch the program.";

            LOGGER.error(message, throwable);

            throw new IllegalStateException(message, throwable);
        }

        return newBlocks;
    }
}
