package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Arrays;
import java.util.List;

public class BackupPlan implements Plan {
    private BackupSnapshotPhase backupSnapshotPhase;
    private UploadBackupPhase uploadBackupPhase;

    public BackupPlan(BackupContext backupContext,
                      int servers,
                      CassandraTasks cassandraTasks,
                      EventBus eventBus,
                      ClusterTaskOfferRequirementProvider provider) {
        this.backupSnapshotPhase = new BackupSnapshotPhase(backupContext, servers, cassandraTasks, eventBus, provider, 0);
        this.uploadBackupPhase = new UploadBackupPhase(backupContext, servers, cassandraTasks, eventBus, provider, 1);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return Arrays.asList(backupSnapshotPhase, uploadBackupPhase);
    }


    @Override
    public boolean isComplete() {
        return uploadBackupPhase.isComplete();
    }
}
