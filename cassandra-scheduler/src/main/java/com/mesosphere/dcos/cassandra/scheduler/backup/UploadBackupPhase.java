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
 * During UploadBackupPhase, snapshotted data will be uploaded to external location.
 */
public class UploadBackupPhase extends AbstractClusterTaskPhase<UploadBackupBlock, BackupContext> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UploadBackupPhase.class);

    public UploadBackupPhase(
            BackupContext context,
            int servers,
            CassandraTasks cassandraTasks,
            EventBus eventBus,
            ClusterTaskOfferRequirementProvider provider) {
        super(context, servers, cassandraTasks, eventBus, provider);
    }

    protected List<UploadBackupBlock> createBlocks() {
        final List<UploadBackupBlock> newBlocks = new ArrayList<>(servers);
        final List<String> createdBlocks =
                new ArrayList<>(cassandraTasks.getBackupUploadTasks().keySet());

        try {
            for (int i = 0; i < servers; i++) {
                String taskId = (i < createdBlocks.size()) ? createdBlocks.get(i)
                        : cassandraTasks.createBackupUploadTask(i, context).getId();
                final UploadBackupBlock block = UploadBackupBlock.create(i, taskId,
                        cassandraTasks, provider, context);
                newBlocks.add(block);
                eventBus.register(block);
            }
        } catch (Throwable throwable) {
            String message = "Failed to create UploadBackupPhase this is a" +
                    " fatal exception and the program will now exit. Please " +
                    "verify your scheduler configuration and attempt to " +
                    "relaunch the program.";

            LOGGER.error(message, throwable);

            throw new IllegalStateException(message, throwable);
        }

        return newBlocks;
    }
}
