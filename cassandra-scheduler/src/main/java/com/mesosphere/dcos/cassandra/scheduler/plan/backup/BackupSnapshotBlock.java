package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BackupSnapshotBlock extends AbstractClusterTaskBlock<BackupContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupSnapshotBlock.class);

    public static BackupSnapshotBlock create(
            final String daemon,
            final CassandraTasks cassandraTasks,
            final CassandraOfferRequirementProvider provider,
            final BackupContext context) {
        return new BackupSnapshotBlock(daemon, cassandraTasks, provider,
                context);
    }

    public BackupSnapshotBlock(
            final String daemon,
            final CassandraTasks cassandraTasks,
            final CassandraOfferRequirementProvider provider,
            final BackupContext context) {
        super(daemon, cassandraTasks, provider, context);
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask(BackupContext context)
            throws PersistenceException {
        CassandraDaemonTask daemonTask =
                cassandraTasks.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.Complete);
            return Optional.empty();
        }
        return Optional.of(cassandraTasks.getOrCreateBackupSnapshot(
                daemonTask,
                context));
    }

    @Override
    public String getName() {
        return BackupSnapshotTask.nameForDaemon(daemon);
    }
}
