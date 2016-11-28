package com.mesosphere.dcos.cassandra.scheduler.plan.backup;


import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BackupSchemaBlock extends AbstractClusterTaskBlock<BackupRestoreContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupSchemaBlock.class);

    public static BackupSchemaBlock create(
            final String daemon,
            final CassandraState cassandraState,
            final CassandraOfferRequirementProvider provider,
            final BackupRestoreContext context) {
        return new BackupSchemaBlock(daemon, cassandraState, provider,
                context);
    }

    public BackupSchemaBlock(
            final String daemon,
            final CassandraState cassandraState,
            final CassandraOfferRequirementProvider provider,
            final BackupRestoreContext context) {
        super(daemon, cassandraState, provider, context);
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask(BackupRestoreContext context)
            throws PersistenceException {
        CassandraDaemonTask daemonTask = cassandraState.getDaemons().get(getDaemon());
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for backup schema does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateBackupSchema(
                daemonTask,
                context));
    }

    @Override
    public String getName() {
        return BackupSchemaTask.nameForDaemon(getDaemon());
    }
}
