package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RestoreSnapshotBlock extends AbstractClusterTaskBlock<RestoreContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSnapshotBlock.class);

    public static RestoreSnapshotBlock create(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            RestoreContext context) {
        return new RestoreSnapshotBlock(daemon, cassandraTasks, provider,
                context);
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask(RestoreContext context)
            throws PersistenceException {

        CassandraDaemonTask daemonTask =
                cassandraTasks.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.Complete);
            return Optional.empty();
        }
        return Optional.of(cassandraTasks.getOrCreateRestoreSnapshot(
                daemonTask,
                context));

    }

    public RestoreSnapshotBlock(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            RestoreContext context) {
        super(daemon, cassandraTasks, provider, context);
    }


    @Override
    public String getName() {
        return RestoreSnapshotTask.nameForDaemon(daemon);
    }
}
