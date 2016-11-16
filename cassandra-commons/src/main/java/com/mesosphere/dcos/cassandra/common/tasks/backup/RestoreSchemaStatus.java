package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

/**
 * RestoreSchemaStatus extends CassandraTaskStatus to implement the status
 * Object for RestoreSchemaTask.
 */
public class RestoreSchemaStatus extends CassandraTaskStatus {
    public static RestoreSchemaStatus create(
            final Protos.TaskStatus status) {
        return new RestoreSchemaStatus(status);
    }

    protected RestoreSchemaStatus(final Protos.TaskStatus status) {
        super(status);
    }
}
