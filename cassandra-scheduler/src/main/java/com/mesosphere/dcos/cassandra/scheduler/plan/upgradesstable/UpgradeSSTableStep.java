package com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable;


import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableTask;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskStep;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class UpgradeSSTableStep extends AbstractClusterTaskStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpgradeSSTableStep.class);

    private final UpgradeSSTableContext context;

    public static UpgradeSSTableStep create(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            UpgradeSSTableContext context) {
        return new UpgradeSSTableStep(daemon, cassandraState, provider, context);
    }

    public UpgradeSSTableStep(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            UpgradeSSTableContext context) {
        super(daemon, UpgradeSSTableTask.nameForDaemon(daemon), cassandraState, provider);
        this.context = context;
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask() throws PersistenceException {
        CassandraDaemonTask daemonTask = cassandraState.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for upgradesstable does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateUpgradeSSTable(daemonTask, context));
    }
}