package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;

import java.util.Collections;
import java.util.List;

public class AbstractClusterTaskPhase<C extends ClusterTaskContext> extends DefaultPhase {

    protected final C context;
    protected final CassandraState cassandraState;
    protected final ClusterTaskOfferRequirementProvider provider;

    protected AbstractClusterTaskPhase(
            String name,
            List<Step> steps,
            C context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        super(name, steps, new SerialStrategy<>(), Collections.emptyList());
        this.context = context;
        this.cassandraState = cassandraState;
        this.provider = provider;
    }
}
