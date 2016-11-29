package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskStep;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class UploadBackupStep extends AbstractClusterTaskStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadBackupStep.class);

    private final BackupRestoreContext context;

    public UploadBackupStep(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            BackupRestoreContext context) {
        super(daemon, BackupUploadTask.nameForDaemon(daemon), cassandraState, provider);
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
        return Optional.of(cassandraState.getOrCreateBackupUpload(daemonTask, context));
    }
}
