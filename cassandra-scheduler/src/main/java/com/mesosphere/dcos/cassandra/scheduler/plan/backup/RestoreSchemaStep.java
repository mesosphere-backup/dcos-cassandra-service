package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaTask;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskStep;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RestoreSchemaStep extends AbstractClusterTaskStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreSchemaStep.class);

    private final BackupRestoreContext context;

    public RestoreSchemaStep(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            BackupRestoreContext context) {
        super(daemon, RestoreSchemaTask.nameForDaemon(daemon), cassandraState, provider);
        this.context = context;
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask() throws PersistenceException {
        CassandraDaemonTask daemonTask = cassandraState.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateRestoreSchema(
                daemonTask,
                context));
    }
}
