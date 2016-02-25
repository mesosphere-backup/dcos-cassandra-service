package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import com.mesosphere.dcos.cassandra.scheduler.tasks.Reconciler;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Arrays;
import java.util.List;

public class CassandraDeploy implements Plan {


    public static final CassandraDeploy create(
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client,
            final Reconciler reconciler) {

        return new CassandraDeploy(
                offerRequirementProvider,
                configurationManager,
                cassandraTasks,
                client,
        reconciler);
    }

    private final CassandraDaemonPhase daemonPhase;
    private final CassandraOfferRequirementProvider offerRequirementProvider;
    private final ReconciliationPhase reconciliationPhase;
    private final List<Phase> phases;

    public CassandraDeploy(
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client,
            final Reconciler reconciler) {
        this.offerRequirementProvider = offerRequirementProvider;
        this.daemonPhase = CassandraDaemonPhase.create(
                configurationManager,
                this.offerRequirementProvider,
                cassandraTasks,
                client);
        this.reconciliationPhase = ReconciliationPhase.create(reconciler);
        this.phases = Arrays.asList(reconciliationPhase,daemonPhase);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return phases;
    }

    @Override
    public Phase getCurrentPhase() {
        return (reconciliationPhase.isComplete()) ?
                daemonPhase : reconciliationPhase;
    }

    @Override
    public Status getStatus() {
        return getCurrentPhase().getStatus();
    }

    @Override
    public boolean isComplete() {
        return daemonPhase.isComplete();
    }
}
