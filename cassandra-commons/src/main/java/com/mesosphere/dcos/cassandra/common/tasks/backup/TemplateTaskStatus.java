package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

/**
 * Created by gabriel on 6/9/16.
 */
public class TemplateTaskStatus extends CassandraTaskStatus {

    public static TemplateTaskStatus create(final Protos.TaskStatus status) {
        return new TemplateTaskStatus(status);
    }

    protected TemplateTaskStatus(final Protos.TaskStatus status) {
        super(status);
    }
}
