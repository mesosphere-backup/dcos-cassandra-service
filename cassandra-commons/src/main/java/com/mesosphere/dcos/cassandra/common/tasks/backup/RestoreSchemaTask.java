package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskUtils;

import java.util.Optional;

/**
 * Restore SchemaTask extends CassandraTask to implement a task that
 * restores the schema of a set of key spaces and column families for a
 * Cassandra cluster to a node. The task can only be launched successfully if
 * the CassandraDaemonTask is running on the targeted slave.
 */
public class RestoreSchemaTask extends CassandraTask {

    /**
     * Prefix for the name of RestoreSchemaTasks.
     */
    public static final String NAME_PREFIX = "restoreschema-";

    /**
     * Gets the name of a RestoreSchemaTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  RestoreSchemaTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a RestoreSchemaTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the Schema will be
     *               uploaded.
     * @return The name of the  RestoreSchemaTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    public static RestoreSchemaTask parse(final Protos.TaskInfo info){
        return new RestoreSchemaTask(info);
    }

    public static RestoreSchemaTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final BackupRestoreContext context) {

        CassandraData data = CassandraData.createRestoreSchemaData(
                "",
                context
                        .forNode(daemon.getName())
                        .withLocalLocation(daemon.getVolumePath() + "/data"));

        String name = nameForDaemon(daemon);
        Protos.TaskInfo completedTemplate = Protos.TaskInfo.newBuilder(template)
                .setName(name)
                .setTaskId(TaskUtils.toTaskId(name))
                .setData(data.getBytes())
                .build();

        completedTemplate = org.apache.mesos.offer.TaskUtils.clearTransient(completedTemplate);

        return new RestoreSchemaTask(completedTemplate);
    }

    /**
     * Constructs a new RestoreSchemaTask.
     */
    protected RestoreSchemaTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public RestoreSchemaTask update(Protos.Offer offer) {
        return new RestoreSchemaTask(getBuilder()
                .setSlaveId(offer.getSlaveId())
                .setData(getData().withHostname(offer.getHostname()).getBytes())
                .build());
    }

    @Override
    public RestoreSchemaTask updateId() {
        return new RestoreSchemaTask(getBuilder().setTaskId(createId(getName()))
                .build());
    }

    @Override
    public RestoreSchemaTask update(CassandraTaskStatus status) {
        if (status.getType() == CassandraTask.TYPE.SCHEMA_RESTORE &&
                getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public RestoreSchemaTask update(Protos.TaskState state) {
        return new RestoreSchemaTask(getBuilder().setData(
                getData().withState(state).getBytes()).build());
    }

    @Override
    public RestoreSchemaStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return RestoreSchemaStatus.create(builder
                .setData(CassandraData.createRestoreSchemaStatusData().getBytes())
                .setState(state)
                .build());
    }

    public BackupRestoreContext getBackupRestoreContext() {
        return getData().getBackupRestoreContext();
    }
}
