package com.mesosphere.dcos.cassandra.scheduler.plan.repair;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RepairBlock extends AbstractClusterTaskBlock<RepairContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RepairBlock.class);

    public static RepairBlock create(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            RepairContext context) {
        return new RepairBlock(daemon, cassandraTasks, provider, context);
    }

    public RepairBlock(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            RepairContext context) {
        super(daemon, cassandraTasks, provider, context);
    }


    @Override
    protected Optional<CassandraTask> getOrCreateTask(RepairContext context)
            throws PersistenceException {
        CassandraDaemonTask daemonTask =
                cassandraTasks.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.Complete);
            return Optional.empty();
        }
        return Optional.of(cassandraTasks.getOrCreateRepair(
                daemonTask,
                context));
    }

    @Override
    public String getName() {
        return RepairTask.nameForDaemon(daemon);
    }

    @Override
    public String toString() {
        return "RepairBlock{" +
                "name='" + getName() + '\'' +
                ", id=" + id +
                '}';
    }
}