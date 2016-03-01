package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * During download snapshot phase, snapshotted data will be downloaded to all the cassandra node from
 * external location.
 */
public class DownloadSnapshotPhase extends AbstractClusterTaskPhase<DownloadSnapshotBlock, RestoreContext> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DownloadSnapshotPhase.class);

    public DownloadSnapshotPhase(
            RestoreContext context,
            int servers,
            CassandraTasks cassandraTasks,
            EventBus eventBus,
            ClusterTaskOfferRequirementProvider provider) {
        super(context, servers, cassandraTasks, eventBus, provider);
    }

    protected List<DownloadSnapshotBlock> createBlocks() {
        final List<DownloadSnapshotBlock> newBlocks = new ArrayList<>(super.servers);
        final List<String> createdBlocks =
                new ArrayList<>(cassandraTasks.getBackupSnapshotTasks().keySet());

        try {
            for (int i = 0; i < servers; i++) {
                String taskId = (i < createdBlocks.size()) ? createdBlocks.get(i)
                        : cassandraTasks.createDownloadSnapshotTask(i, context).getId();
                final DownloadSnapshotBlock block = DownloadSnapshotBlock.create(i, taskId,
                        cassandraTasks, provider, context);
                newBlocks.add(block);
                eventBus.register(block);
            }
        } catch (Throwable throwable) {
            String message = "Failed to create DownloadSnapshotPhase this is a" +
                    " fatal exception and the program will now exit. Please " +
                    "verify your scheduler configuration and attempt to " +
                    "relaunch the program.";

            LOGGER.error(message, throwable);

            throw new IllegalStateException(message, throwable);
        }

        return newBlocks;
    }
}
