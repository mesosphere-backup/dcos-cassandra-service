package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskUtils;

import java.util.Optional;

/**
 * BackupSchemaTask extends CassandraTask to implement a task that
 * back's up schema for cassandra process.
 * The task can only be launched successfully if the CassandraDaemonTask is
 * running on the targeted slave.
 * Back's up schema for all customer created keyspaces
 * and column families.
 */
public class BackupSchemaTask extends CassandraTask{

    /**
     * The name prefix for BackupSchemaTasks.
     */
    public static final String NAME_PREFIX = "backupschema-";

    /**
     * Gets the name of a BackupSchemaTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the BackupSchemaTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a BackupSchemaTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the schema will be
     *               backed up.
     * @return The name of the BackupSchemaTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }


    public static BackupSchemaTask parse(final Protos.TaskInfo info) {
        return new BackupSchemaTask(info);
    }


    public static BackupSchemaTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final BackupRestoreContext context) {

        String name = nameForDaemon(daemon);
        CassandraData data = CassandraData.createBackupSchemaData(
                "",
                context
                        .forNode(daemon.getName())
                        .withLocalLocation(daemon.getVolumePath() + "/data"));

        Protos.TaskInfo completedTemplate = Protos.TaskInfo.newBuilder(template)
                .setName(name)
                .setTaskId(TaskUtils.toTaskId(name))
                .setData(data.getBytes())
                .build();

        completedTemplate = org.apache.mesos.offer.TaskUtils.clearTransient(completedTemplate);

        return new BackupSchemaTask(completedTemplate);
    }

    /**
     * Constructs a new BackupSchemaTask.
     */
    protected BackupSchemaTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public BackupSchemaTask update(Protos.Offer offer) {
        return new BackupSchemaTask(getBuilder()
                .setSlaveId(offer.getSlaveId())
                .setData(getData().withHostname(offer.getHostname()).getBytes())
                .build());
    }

    @Override
    public BackupSchemaTask updateId() {
        return new BackupSchemaTask(getBuilder().setTaskId(createId(getName()))
                .build());
    }

    @Override
    public BackupSchemaTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.BACKUP_SCHEMA &&
                getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public BackupSchemaTask update(Protos.TaskState state) {
        return new BackupSchemaTask(getBuilder().setData(
                getData().withState(state).getBytes()).build());
    }

    @Override
    public BackupSchemaStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return BackupSchemaStatus.create(builder
                .setData(CassandraData.createBackupSchemaStatusData().getBytes())
                .setState(state)
                .build());
    }


    public BackupRestoreContext getBackupRestoreContext() {
        return getData().getBackupRestoreContext();
    }
}
