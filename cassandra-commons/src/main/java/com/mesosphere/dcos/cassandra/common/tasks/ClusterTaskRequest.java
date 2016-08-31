package com.mesosphere.dcos.cassandra.common.tasks;

/**
 * Interface which defines the request to a scheduler to perform some kind of task (backup, restore)
 * against the cluster.
 */
public interface ClusterTaskRequest {
    public boolean isValid();
}
