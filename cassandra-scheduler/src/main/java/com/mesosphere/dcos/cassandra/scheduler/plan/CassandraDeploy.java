package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Arrays;
import java.util.List;

public class CassandraDeploy implements Plan {
    private final CassandraDaemonPhase phase;
    private final CassandraOfferRequirementProvider offerRequirementProvider;

    public static final CassandraDeploy create(
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final ConfigurationManager configurationManager,
            final EventBus eventBus,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {

        return new CassandraDeploy(
                offerRequirementProvider,
                configurationManager,
                eventBus,
                cassandraTasks,
                client);
    }

    public CassandraDeploy(
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final ConfigurationManager configurationManager,
            final EventBus eventBus,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {
        this.offerRequirementProvider = offerRequirementProvider;
        this.phase = new CassandraDaemonPhase(
                configurationManager,
                this.offerRequirementProvider,
                eventBus,
                cassandraTasks,
                client);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return Arrays.asList(phase);
    }

    @Override
    public Phase getCurrentPhase() {
        return phase;
    }

    @Override
    public Status getStatus() {
        return getCurrentPhase().getStatus();
    }

    @Override
    public boolean isComplete() {
        return phase.isComplete();
    }
}
