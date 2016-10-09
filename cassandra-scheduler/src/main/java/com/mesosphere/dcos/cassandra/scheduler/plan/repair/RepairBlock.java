package com.mesosphere.dcos.cassandra.scheduler.plan.repair;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RepairBlock extends AbstractClusterTaskBlock<RepairContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RepairBlock.class);

    public static RepairBlock create(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            RepairContext context) {
        return new RepairBlock(daemon, cassandraState, provider, context);
    }

    public RepairBlock(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            RepairContext context) {
        super(daemon, cassandraState, provider, context);
    }


    @Override
    protected Optional<CassandraTask> getOrCreateTask(RepairContext context)
            throws PersistenceException {
        CassandraDaemonTask daemonTask =
                cassandraState.getDaemons().get(getDaemon());
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateRepair(
                daemonTask,
                context));
    }

    @Override
    public String getName() {
        return RepairTask.nameForDaemon(getDaemon());
    }

    @Override
    public String toString() {
        return "RepairBlock{" +
                "name='" + getName() + '\'' +
                ", id=" + getId() +
                '}';
    }
}