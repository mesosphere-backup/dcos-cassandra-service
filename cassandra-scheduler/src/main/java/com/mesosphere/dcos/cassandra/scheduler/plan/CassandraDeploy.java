package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.ReconciliationPhase;
import org.apache.mesos.scheduler.plan.Stage;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CassandraDeploy implements Stage {


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
        this.phases = Arrays.asList(reconciliationPhase, daemonPhase);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return phases;
    }

    @Override
    public List<String> getErrors() {
        return Collections.emptyList();
    }


    @Override
    public boolean isComplete() {
        return daemonPhase.isComplete();
    }
}
