package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

public class TemplateTaskStatus extends CassandraTaskStatus {

    public static TemplateTaskStatus create(final Protos.TaskStatus status) {
        return new TemplateTaskStatus(status);
    }

    protected TemplateTaskStatus(final Protos.TaskStatus status) {
        super(status);
    }
}
