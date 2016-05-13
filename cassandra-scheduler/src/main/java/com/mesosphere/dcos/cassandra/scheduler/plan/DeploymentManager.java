package com.mesosphere.dcos.cassandra.scheduler.plan;


import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.ReconciliationPhase;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class DeploymentManager {

    public static final DeploymentManager create(
            final CassandraOfferRequirementProvider provider,
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final SchedulerClient client,
            final Reconciler reconciler,
            final SeedsManager seeds,
            final ExecutorService executor) {
        return new DeploymentManager(provider,
                configurationManager,
                cassandraTasks,
                client,
                reconciler,
                seeds,
                executor);
    }

    private final ReconciliationPhase reconciliation;
    private final CassandraDaemonPhase deploy;
    private final SyncDataCenterPhase syncDc;

    public DeploymentManager(
            final CassandraOfferRequirementProvider provider,
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final SchedulerClient client,
            final Reconciler reconciler,
            final SeedsManager seeds,
            final ExecutorService executor) {
        this.deploy = CassandraDaemonPhase.create(
                configurationManager,
                cassandraTasks,
                provider,
                client);
        this.reconciliation = ReconciliationPhase.create(reconciler,
                cassandraTasks);

        this.syncDc = SyncDataCenterPhase.create(seeds, executor);
    }

    public List<? extends Phase> getPhases() {

        return Arrays.asList(reconciliation, syncDc, deploy);
    }

    public List<String> getErrors() {
        return deploy.getErrors();
    }

    public boolean isComplete() {
        return deploy.isComplete();
    }
}
