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
public class UploadBackupPhase implements Phase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UploadBackupPhase.class);

    private final EventBus eventBus;
    private final CassandraTasks cassandraTasks;
    private int servers;
    private List<UploadBackupBlock> blocks;
    private BackupContext backupContext;
    private ClusterTaskOfferRequirementProvider provider;
    private PhaseStrategy strategy = new DefaultInstallStrategy(this);

    public UploadBackupPhase(
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

    private List<UploadBackupBlock> createBlocks() {
        final List<UploadBackupBlock> newBlocks = new ArrayList<>(servers);
        final List<String> createdBlocks =
                new ArrayList<>(cassandraTasks.getBackupUploadTasks().keySet());

        try {
            for (int i = 0; i < servers; i++) {
                String taskId = (i < createdBlocks.size()) ? createdBlocks.get(i)
                        : cassandraTasks.createBackupUploadTask(i, backupContext).getId();
                final UploadBackupBlock block = UploadBackupBlock.create(i, taskId,
                        cassandraTasks, provider, backupContext);
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

    @Override
    public PhaseStrategy getStrategy() {
        return strategy;
    }
}
