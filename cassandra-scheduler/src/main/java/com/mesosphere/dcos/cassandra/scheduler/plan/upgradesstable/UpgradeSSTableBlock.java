package com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable;


import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableTask;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class UpgradeSSTableBlock extends AbstractClusterTaskBlock<UpgradeSSTableContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            UpgradeSSTableBlock.class);

    public static UpgradeSSTableBlock create(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            UpgradeSSTableContext context) {
        return new UpgradeSSTableBlock(daemon, cassandraState, provider, context);
    }

    public UpgradeSSTableBlock(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            UpgradeSSTableContext context) {
        super(daemon, cassandraState, provider, context);
    }


    @Override
    protected Optional<CassandraTask> getOrCreateTask(UpgradeSSTableContext context)
            throws PersistenceException {
        CassandraDaemonTask daemonTask =
                cassandraState.getDaemons().get(getDaemon());
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for upgradesstable does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateUpgradeSSTable(
                daemonTask,
                context));
    }

    @Override
    public String getName() {
        return UpgradeSSTableTask.nameForDaemon(getDaemon());
    }

    @Override
    public String toString() {
        return "UpgradeSSTableBlock{" +
                "name='" + getName() + '\'' +
                ", id=" + getId() +
                '}';
    }
}