package com.mesosphere.dcos.cassandra.scheduler.plan.cleanup;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskStep;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class CleanupStep extends AbstractClusterTaskStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanupStep.class);

    private final CleanupContext context;

    public static CleanupStep create(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            CleanupContext context) {
        return new CleanupStep(daemon, cassandraState, provider, context);
    }

    public CleanupStep(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            CleanupContext context) {
        super(daemon, CleanupTask.nameForDaemon(daemon), cassandraState, provider);
        this.context = context;
    }


    @Override
    protected Optional<CassandraTask> getOrCreateTask() throws PersistenceException {
        CassandraDaemonTask daemonTask = cassandraState.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateCleanup(daemonTask, context));
    }
}