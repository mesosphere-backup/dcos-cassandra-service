package com.mesosphere.dcos.cassandra.scheduler.plan;


import org.apache.mesos.scheduler.plan.Block;

public interface CassandraBlock extends Block {
    String getTaskId();
}
