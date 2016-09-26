package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RestoreSnapshotBlock extends AbstractClusterTaskBlock<BackupRestoreContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSnapshotBlock.class);

    public static RestoreSnapshotBlock create(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            BackupRestoreContext context) {
        return new RestoreSnapshotBlock(daemon, cassandraState, provider,
                context);
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask(BackupRestoreContext context)
            throws PersistenceException {

        CassandraDaemonTask daemonTask =
                cassandraState.getDaemons().get(getDaemon());
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateRestoreSnapshot(
                daemonTask,
                context));

    }

    public RestoreSnapshotBlock(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            BackupRestoreContext context) {
        super(daemon, cassandraState, provider, context);
    }


    @Override
    public String getName() {
        return RestoreSnapshotTask.nameForDaemon(getDaemon());
    }
}
