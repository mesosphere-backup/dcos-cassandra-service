package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * During snapshot phase, data will be snapshotted across all cassandra nodes.
 */
public class BackupSnapshotPhase implements Phase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BackupSnapshotPhase.class);

    private int servers;
    private final EventBus eventBus;
    private BackupContext backupContext;
    private List<BackupSnapshotBlock> blocks;
    private final CassandraTasks cassandraTasks;
    private ClusterTaskOfferRequirementProvider provider;

    public BackupSnapshotPhase(
            BackupContext backupContext,
            int servers,
            CassandraTasks cassandraTasks,
            EventBus eventBus,
            ClusterTaskOfferRequirementProvider provider) {
        this.servers = servers;
        this.eventBus = eventBus;
        this.provider = provider;
        this.backupContext = backupContext;
        this.cassandraTasks = cassandraTasks;
        this.blocks = createBlocks();
    }

    private List<BackupSnapshotBlock> createBlocks() {
        final List<BackupSnapshotBlock> newBlocks = new ArrayList<>(servers);
        final List<String> createdBlocks =
                new ArrayList<>(cassandraTasks.getBackupSnapshotTasks().keySet());

        try {
            for (int i = 0; i < servers; i++) {
                String taskId = (i < createdBlocks.size()) ? createdBlocks.get(i)
                        : cassandraTasks.createBackupSnapshotTask(i, backupContext).getId();
                final BackupSnapshotBlock block = BackupSnapshotBlock.create(i, taskId,
                        cassandraTasks, provider, backupContext);
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

    @Override
    public List<? extends Block> getBlocks() {
        return blocks;
    }

    @Override
    public Block getCurrentBlock() {
        Block currentBlock = null;
        if (!CollectionUtils.isEmpty(blocks)) {
            for (Block block : blocks) {
                if (!block.isComplete()) {
                    currentBlock = block;
                    break;
                }
            }
        }

        return currentBlock;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public Status getStatus() {
        return getCurrentBlock().getStatus();
    }

    @Override
    public boolean isComplete() {
        for (Block block : blocks) {
            if (!block.isComplete()) {
                return false;
            }
        }

        return true;
    }
}
