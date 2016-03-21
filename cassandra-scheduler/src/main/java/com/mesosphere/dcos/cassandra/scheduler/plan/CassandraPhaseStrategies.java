package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupSnapshotPhase;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.DownloadSnapshotPhase;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreSnapshotPhase;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.UploadBackupPhase;
import org.apache.mesos.scheduler.plan.*;

public class CassandraPhaseStrategies implements PhaseStrategyFactory {

    private final Class<?> phaseStrategy;

    @Inject
    public CassandraPhaseStrategies(
            @Named("ConfiguredPhaseStrategy") final String phaseStrategy) {
        try {
            this.phaseStrategy =
                    this.getClass().getClassLoader().loadClass(phaseStrategy);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format(
                    "Failed to load class for phaseStrategy $s",
                    phaseStrategy
            ), e);
        }
    }

    ;

    @Override
    public PhaseStrategy getStrategy(Phase phase) {
        if (phase instanceof EmptyPhase) {
            return NoOpPhaseStrategy.get();
        } else if (phase instanceof ReconciliationPhase) {
            return ReconciliationStrategy.create((ReconciliationPhase) phase);
        } else if (phase instanceof BackupSnapshotPhase ||
                phase instanceof UploadBackupPhase ||
                phase instanceof DownloadSnapshotPhase ||
                phase instanceof RestoreSnapshotPhase) {
            return new DefaultInstallStrategy(phase);
        } else {
            try {
                return (PhaseStrategy)
                        phaseStrategy.getConstructor(Phase.class).newInstance(
                                phase);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to PhaseStrategy", ex);
            }
        }
    }
}
