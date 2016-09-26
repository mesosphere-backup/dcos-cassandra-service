package com.mesosphere.dcos.cassandra.scheduler.plan.repair;


import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;

import java.util.*;
import java.util.stream.Collectors;

public class RepairPhase extends AbstractClusterTaskPhase<RepairBlock, RepairContext> {

    public RepairPhase(
            RepairContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        super(context, cassandraState, provider);
    }

    protected List<RepairBlock> createBlocks() {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().filter(
                deamon -> nodes.contains(deamon)
        ).map(daemon -> RepairBlock.create(
                daemon,
                cassandraState,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {return "Repair";}
}
