package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * During backup schema phase, schema for cassandra node will be backed up.
 */
public class BackupSchemaPhase extends AbstractClusterTaskPhase<BackupSchemaBlock, BackupRestoreContext> {

    public BackupSchemaPhase(
            BackupRestoreContext BackupRestoreContext,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        super(BackupRestoreContext, cassandraState, provider);
    }

    protected List<BackupSchemaBlock> createBlocks() {
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().map(daemon -> BackupSchemaBlock.create(
                daemon,
                cassandraState,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "BackupSchema";
    }
}
