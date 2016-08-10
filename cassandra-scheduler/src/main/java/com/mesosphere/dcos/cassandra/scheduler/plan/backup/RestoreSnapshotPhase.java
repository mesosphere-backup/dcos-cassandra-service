package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * During download snapshot phase, snapshotted data will be downloaded to all the cassandra node from
 * external location.
 */
public class RestoreSnapshotPhase extends AbstractClusterTaskPhase<RestoreSnapshotBlock, BackupRestoreContext> {

    public RestoreSnapshotPhase(
            BackupRestoreContext context,
            CassandraTasks cassandraTasks,
            ClusterTaskOfferRequirementProvider provider) {
        super(context, cassandraTasks, provider);
    }

    protected List<RestoreSnapshotBlock> createBlocks() {
        final List<String> daemons =
                new ArrayList<>(cassandraTasks.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().map(daemon -> RestoreSnapshotBlock.create(
                daemon,
                cassandraTasks,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "Restore";
    }
}
