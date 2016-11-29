package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
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

public class RestoreManager extends ClusterTaskManager<BackupRestoreRequest, BackupRestoreContext> {
    static final String RESTORE_KEY = "restore";

    private CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public RestoreManager(
            final CassandraState cassandraState,
            final ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, RESTORE_KEY, BackupRestoreContext.class);
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
                createDownloadSnapshotPhase(context, cassandraState, provider),
                createRestoreSnapshotPhase(context, cassandraState, provider));
    }

    @Override
    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getDownloadSnapshotTasks().keySet());
        cassandraState.remove(cassandraState.getRestoreSnapshotTasks().keySet());
    }

    private static Phase createDownloadSnapshotPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> DownloadSnapshotStep.create(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("Download", steps, new SerialStrategy<>(), Collections.emptyList());
    }

    private static Phase createRestoreSnapshotPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> RestoreSnapshotStep.create(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("Restore", steps, new SerialStrategy<>(), Collections.emptyList());
    }
}
