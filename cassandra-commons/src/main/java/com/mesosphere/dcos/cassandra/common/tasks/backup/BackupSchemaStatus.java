package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

/**
 * BackupSchemaStatus extends CassandraTaskStatus to implement the status
 * Object for the BackupSchema task.
 */
public class BackupSchemaStatus extends CassandraTaskStatus {

    public static BackupSchemaStatus create(final Protos.TaskStatus status) {
        return new BackupSchemaStatus(status);
    }

    protected BackupSchemaStatus(final Protos.TaskStatus status) {
        super(status);
    }
}
