package com.mesosphere.dcos.cassandra.scheduler.plan.cleanup;


import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;

import java.util.*;
import java.util.stream.Collectors;

public class CleanupPhase extends AbstractClusterTaskPhase<CleanupBlock, CleanupContext> {

    public CleanupPhase(
            CleanupContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        super(context, cassandraState, provider);
    }

    @Override
    protected List<CleanupBlock> createBlocks() {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().filter(
                deamon -> nodes.contains(deamon)
        ).map(daemon -> CleanupBlock.create(
                daemon,
                cassandraState,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "Cleanup";
    }
}
