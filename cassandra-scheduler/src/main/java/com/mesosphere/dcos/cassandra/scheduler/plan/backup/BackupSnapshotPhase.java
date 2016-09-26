package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * During snapshot phase, data will be snapshotted across all cassandra nodes.
 */
public class BackupSnapshotPhase extends AbstractClusterTaskPhase<BackupSnapshotBlock, BackupRestoreContext> {
    public BackupSnapshotPhase(
            BackupRestoreContext BackupRestoreContext,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        super(BackupRestoreContext, cassandraState, provider);
    }

    protected List<BackupSnapshotBlock> createBlocks() {
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().map(daemon -> BackupSnapshotBlock.create(
                daemon,
                cassandraState,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "Snapshot";
    }
}
