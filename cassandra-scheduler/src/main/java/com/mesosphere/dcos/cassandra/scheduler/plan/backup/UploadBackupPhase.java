package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * During UploadBackupPhase, snapshotted data will be uploaded to external location.
 */
public class UploadBackupPhase extends AbstractClusterTaskPhase<UploadBackupBlock, BackupRestoreContext> {

    public UploadBackupPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        super(context, cassandraState, provider);
    }

    protected List<UploadBackupBlock> createBlocks() {
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().map(daemon -> UploadBackupBlock.create(
                daemon,
                cassandraState,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "Upload";
    }
}
