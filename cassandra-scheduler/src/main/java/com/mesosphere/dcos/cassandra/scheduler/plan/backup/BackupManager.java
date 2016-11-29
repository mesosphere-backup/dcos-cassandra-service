package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.resources.BackupRestoreRequest;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.apache.mesos.state.StateStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * BackupManager is responsible for orchestrating cluster-wide backup.
 * It also ensures that only one backup can run an anytime. For each new backup
 * a new BackupPlan is created, which will assist in orchestration.
 */
public class BackupManager extends ClusterTaskManager<BackupRestoreRequest, BackupRestoreContext> {
    static final String BACKUP_KEY = "backup";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public BackupManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, BACKUP_KEY, BackupRestoreContext.class);
        this.provider = provider;
        this.cassandraState = cassandraState;
        restore();
    }

    @Override
    protected BackupRestoreContext toContext(BackupRestoreRequest request) {
        return request.toContext();
    }

    @Override
    protected List<Phase> createPhases(BackupRestoreContext context) {
        return Arrays.asList(
                createBackupSnapshotPhase(context, cassandraState, provider),
                createUploadBackupPhase(context, cassandraState, provider),
                createBackupSchemaPhase(context, cassandraState, provider));
    }

    @Override
    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getBackupSnapshotTasks().keySet());
        cassandraState.remove(cassandraState.getBackupUploadTasks().keySet());
        cassandraState.remove(cassandraState.getBackupSchemaTasks().keySet());
    }

    private static Phase createBackupSnapshotPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> BackupSnapshotStep.create(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("Snapshot", steps, new SerialStrategy<>(), Collections.emptyList());
    }

    private static Phase createUploadBackupPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> UploadBackupStep.create(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("Upload", steps, new SerialStrategy<>(), Collections.emptyList());
    }

    private static Phase createBackupSchemaPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> BackupSchemaStep.create(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("BackupSchema", steps, new SerialStrategy<>(), Collections.emptyList());
    }
}