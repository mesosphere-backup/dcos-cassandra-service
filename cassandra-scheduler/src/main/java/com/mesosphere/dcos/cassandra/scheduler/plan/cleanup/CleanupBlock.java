package com.mesosphere.dcos.cassandra.scheduler.plan.cleanup;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class CleanupBlock extends AbstractClusterTaskBlock<CleanupContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CleanupBlock.class);

    public static CleanupBlock create(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            CleanupContext context) {
        return new CleanupBlock(daemon, cassandraTasks, provider, context);
    }

    public CleanupBlock(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            CleanupContext context) {
        super(daemon, cassandraTasks, provider, context);
    }


    @Override
    protected Optional<CassandraTask> getOrCreateTask(CleanupContext context)
            throws PersistenceException {
        CassandraDaemonTask daemonTask =
                cassandraTasks.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.Complete);
            return Optional.empty();
        }
        return Optional.of(cassandraTasks.getOrCreateCleanup(
                daemonTask,
                context));
    }

    @Override
    public String getName() {
        return CleanupTask.nameForDaemon(daemon);
    }

    @Override
    public String toString() {
        return "CleanupBlock{" +
                "name='" + getName() + '\'' +
                ", id=" + id +
                '}';
    }
}