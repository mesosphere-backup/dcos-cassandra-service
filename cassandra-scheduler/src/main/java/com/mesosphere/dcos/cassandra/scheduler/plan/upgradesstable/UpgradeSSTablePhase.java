package com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable;


import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;

import java.util.*;
import java.util.stream.Collectors;

public class UpgradeSSTablePhase extends AbstractClusterTaskPhase<UpgradeSSTableBlock, UpgradeSSTableContext> {

    public UpgradeSSTablePhase(
            UpgradeSSTableContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        super(context, cassandraState, provider);
    }

    @Override
    protected List<UpgradeSSTableBlock> createBlocks() {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().filter(
                daemon -> nodes.contains(daemon)
        ).map(daemon -> UpgradeSSTableBlock.create(
                daemon,
                cassandraState,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "UpgradeSSTable";
    }
}
