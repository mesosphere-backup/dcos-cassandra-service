package com.mesosphere.dcos.cassandra.scheduler.backup;

import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Arrays;
import java.util.List;

public class BackupPlan implements Plan {
    private SnapshotPhase snapshotPhase;
    private UploadBackupPhase uploadBackupPhase;
    private BackupContext backupContext;

    public BackupPlan(BackupContext backupContext) {
        this.backupContext = backupContext;
        this.snapshotPhase = new SnapshotPhase(backupContext);
        this.uploadBackupPhase = new UploadBackupPhase(backupContext);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return Arrays.asList(snapshotPhase, uploadBackupPhase);
    }

    @Override
    public Phase getCurrentPhase() {
        return !snapshotPhase.isComplete() ? snapshotPhase : uploadBackupPhase;
    }

    @Override
    public Status getStatus() {
        return getCurrentPhase().getStatus();
    }

    @Override
    public boolean isComplete() {
        return uploadBackupPhase.isComplete();
    }
}
