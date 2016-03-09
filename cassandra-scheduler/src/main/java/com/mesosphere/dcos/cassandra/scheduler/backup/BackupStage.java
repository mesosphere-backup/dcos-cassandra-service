package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Stage;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BackupStage implements Stage {
    private BackupSnapshotPhase backupSnapshotPhase;
    private UploadBackupPhase uploadBackupPhase;

    public BackupStage(BackupContext backupContext,
                       CassandraTasks cassandraTasks,
                       ClusterTaskOfferRequirementProvider provider) {
        this.backupSnapshotPhase =
                new BackupSnapshotPhase(backupContext, cassandraTasks,
                        provider);
        this.uploadBackupPhase =
                new UploadBackupPhase(backupContext, cassandraTasks, provider);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return Arrays.asList(backupSnapshotPhase, uploadBackupPhase);
    }

    @Override
    public List<String> getErrors() {
        return Collections.emptyList();
    }

    @Override
    public boolean isComplete() {
        return uploadBackupPhase.isComplete();
    }
}
