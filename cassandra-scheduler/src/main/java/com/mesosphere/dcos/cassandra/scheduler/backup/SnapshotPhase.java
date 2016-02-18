package com.mesosphere.dcos.cassandra.scheduler.backup;

import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Status;

import java.util.List;

/**
 * During snapshot phase, data will be snapshotted across all cassandra nodes.
 */
public class SnapshotPhase implements Phase {
    private List<SnapshotBlock> blocks;
    private BackupContext backupContext;

    public SnapshotPhase(BackupContext backupContext) {
        this.backupContext = backupContext;
    }

    @Override
    public List<? extends Block> getBlocks() {
        return null;
    }

    @Override
    public Block getCurrentBlock() {
        return null;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public boolean isComplete() {
        return false;
    }
}
