package com.mesosphere.dcos.cassandra.scheduler.plan;


import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.DefaultObservable;
import org.apache.mesos.scheduler.Observable;
import org.apache.mesos.scheduler.Observer;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.ReconciliationPhase;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class DeploymentManager extends DefaultObservable implements Observer {

    public static final DeploymentManager create(
            final PersistentOfferRequirementProvider provider,
            final ConfigurationManager configurationManager,
            final DefaultConfigurationManager defaultConfigurationManager,
            final CassandraState cassandraState,
            final SchedulerClient client,
            final Reconciler reconciler,
            final SeedsManager seeds,
            final ExecutorService executor) throws ConfigStoreException {
        return new DeploymentManager(provider,
                defaultConfigurationManager,
                cassandraState,
                client,
                reconciler,
                seeds,
                executor);
    }

    private final ReconciliationPhase reconciliation;
    private final CassandraDaemonPhase deploy;
    private final SyncDataCenterPhase syncDc;

    public DeploymentManager(
            final PersistentOfferRequirementProvider provider,
            final DefaultConfigurationManager defaultConfigurationManager,
            final CassandraState cassandraState,
            final SchedulerClient client,
            final Reconciler reconciler,
            final SeedsManager seeds,
            final ExecutorService executor) throws ConfigStoreException {
        this.deploy = CassandraDaemonPhase.create(
                cassandraState,
                provider,
                client,
                defaultConfigurationManager);
        this.reconciliation = ReconciliationPhase.create(reconciler);
        this.syncDc = SyncDataCenterPhase.create(seeds, executor);

        this.deploy.subscribe(this);
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

    @Override
    public void update(Observable observable) {
        notifyObservers();
    }
}
