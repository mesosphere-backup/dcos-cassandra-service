package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class DownloadSnapshotBlock extends AbstractClusterTaskBlock<BackupRestoreContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            DownloadSnapshotBlock.class);

    public static DownloadSnapshotBlock create(
            final String daemon,
            final CassandraTasks cassandraTasks,
            final CassandraOfferRequirementProvider provider,
            final BackupRestoreContext context) {
        return new DownloadSnapshotBlock(daemon, cassandraTasks, provider,
                context);
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask(BackupRestoreContext context)
            throws PersistenceException {
        CassandraDaemonTask daemonTask =
                cassandraTasks.getDaemons().get(getDaemon());
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraTasks.getOrCreateSnapshotDownload(
                daemonTask,
                context));
    }

    public DownloadSnapshotBlock(
            final String daemon,
            final CassandraTasks cassandraTasks,
            final CassandraOfferRequirementProvider provider,
            final BackupRestoreContext context) {
        super(daemon, cassandraTasks, provider, context);
    }

    @Override
    public String getName() {
        return DownloadSnapshotTask.nameForDaemon(getDaemon());
    }
}
