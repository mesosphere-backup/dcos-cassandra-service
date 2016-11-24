package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskStep;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BackupSnapshotStep extends AbstractClusterTaskStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupSnapshotStep.class);

    private final BackupRestoreContext context;

    public static BackupSnapshotStep create(
            final String daemon,
            final CassandraState cassandraState,
            final CassandraOfferRequirementProvider provider,
            final BackupRestoreContext context) {
        return new BackupSnapshotStep(daemon, cassandraState, provider, context);
    }

    public BackupSnapshotStep(
            final String daemon,
            final CassandraState cassandraState,
            final CassandraOfferRequirementProvider provider,
            final BackupRestoreContext context) {
        super(daemon, BackupSnapshotTask.nameForDaemon(daemon), cassandraState, provider);
        this.context = context;
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask()
            throws PersistenceException {
        CassandraDaemonTask daemonTask = cassandraState.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateBackupSnapshot(daemonTask, context));
    }
}
