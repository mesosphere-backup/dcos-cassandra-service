package com.mesosphere.dcos.cassandra.scheduler.plan.cleanup;

import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.resources.CleanupRequest;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

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

public class CleanupManager extends ClusterTaskManager<CleanupRequest, CleanupContext> {
    static final String CLEANUP_KEY = "cleanup";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public CleanupManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, CLEANUP_KEY, CleanupContext.class);
        this.provider = provider;
        this.cassandraState = cassandraState;
        restore();
    }

    @Override
    protected CleanupContext toContext(CleanupRequest request) {
        return request.toContext(cassandraState);
    }

    @Override
    protected List<Phase> createPhases(CleanupContext context) {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .filter(daemon -> nodes.contains(daemon))
                .map(daemon -> new CleanupStep(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return Arrays.asList(new DefaultPhase("Cleanup", steps, new SerialStrategy<>(), Collections.emptyList()));
    }

    @Override
    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getCleanupTasks().keySet());
    }
}