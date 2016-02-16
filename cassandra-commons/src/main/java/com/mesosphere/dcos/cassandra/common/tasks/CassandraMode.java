package com.mesosphere.dcos.cassandra.common.tasks;


public enum CassandraMode {
    STARTING,
    NORMAL,
    JOINING,
    LEAVING,
    DECOMMISSIONED,
    MOVING,
    DRAINING,
    DRAINED
}
