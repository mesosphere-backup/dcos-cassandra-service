package com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.scheduler.resources.UpgradeSSTableRequest;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.apache.mesos.state.StateStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UpgradeSSTableManager extends ClusterTaskManager<UpgradeSSTableRequest, UpgradeSSTableContext> {
    static final String UPGRADESSTABLE_KEY = "upgradesstable";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public UpgradeSSTableManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, UPGRADESSTABLE_KEY, UpgradeSSTableContext.class);
        this.cassandraState = cassandraState;
        this.provider = provider;
        restore();
    }

    protected UpgradeSSTableContext toContext(UpgradeSSTableRequest request) {
        return request.toContext(cassandraState);
    }

    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getUpgradeSSTableTasks().keySet());
    }

    protected List<Phase> createPhases(UpgradeSSTableContext context) {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .filter(daemon -> nodes.contains(daemon))
                .map(daemon -> UpgradeSSTableStep.create(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return Arrays.asList(new DefaultPhase("UpgradeSSTable", steps, new SerialStrategy<>(), Collections.emptyList()));
    }
}