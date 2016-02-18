package com.mesosphere.dcos.cassandra.scheduler.backup;

import org.apache.log4j.lf5.viewer.configure.ConfigurationManager;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Status;

import java.util.List;

/**
 * During UploadBackupPhase, snapshotted data will be uploaded to external location.
 */
public class UploadBackupPhase implements Phase {
    private List<UploadBackupBlock> blocks;
    private BackupContext backupContext;
    private ConfigurationManager configurationManager;

    public UploadBackupPhase(BackupContext backupContext) {
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
