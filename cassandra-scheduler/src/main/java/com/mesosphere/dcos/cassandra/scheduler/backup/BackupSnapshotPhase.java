package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.scheduler.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * During snapshot phase, data will be snapshotted across all cassandra nodes.
 */
public class BackupSnapshotPhase extends AbstractClusterTaskPhase<BackupSnapshotBlock, BackupContext> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BackupSnapshotPhase.class);

    public BackupSnapshotPhase(
            BackupContext backupContext,
            int servers,
            CassandraTasks cassandraTasks,
            EventBus eventBus,
            ClusterTaskOfferRequirementProvider provider) {
        super(backupContext, servers, cassandraTasks, eventBus, provider);
    }

    protected List<BackupSnapshotBlock> createBlocks() {
        final List<BackupSnapshotBlock> newBlocks = new ArrayList<>(super.servers);
        final List<String> createdBlocks =
                new ArrayList<>(cassandraTasks.getBackupSnapshotTasks().keySet());

        try {
            for (int i = 0; i < servers; i++) {
                String taskId = (i < createdBlocks.size()) ? createdBlocks.get(i)
                        : cassandraTasks.createBackupSnapshotTask(i, context).getId();
                final BackupSnapshotBlock block = BackupSnapshotBlock.create(i, taskId,
                        cassandraTasks, provider, context);
                newBlocks.add(block);
                eventBus.register(block);
            }
        } catch (Throwable throwable) {
            String message = "Failed to create BackupSnapshotPhase this is a" +
                    " fatal exception and the program will now exit. Please " +
                    "verify your scheduler configuration and attempt to " +
                    "relaunch the program.";

            LOGGER.error(message, throwable);

            throw new IllegalStateException(message, throwable);
        }

        return newBlocks;
    }
}
