package com.mesosphere.dcos.cassandra.scheduler.plan;


import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.ReconciliationPhase;

import java.util.Arrays;
import java.util.List;

public class DeploymentManager {

    public static final DeploymentManager create(
            final CassandraOfferRequirementProvider provider,
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client,
            final Reconciler reconciler) {
        return new DeploymentManager(provider,
                configurationManager,
                cassandraTasks,
                client,
                reconciler);
    }

    private final ReconciliationPhase reconciliation;
    private final CassandraDaemonPhase deploy;

    public DeploymentManager(
            final CassandraOfferRequirementProvider provider,
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client,
            final Reconciler reconciler) {
        this.deploy = CassandraDaemonPhase.create(
                configurationManager,
                cassandraTasks,
                provider,
                client);
        this.reconciliation = ReconciliationPhase.create(reconciler);
    }

    public List<? extends Phase> getPhases() {

        return Arrays.asList(reconciliation, deploy);
    }

    public List<String> getErrors() {
        return deploy.getErrors();
    }

    public boolean isComplete() {
        return deploy.isComplete();
    }
}
