package com.mesosphere.dcos.cassandra.scheduler.mesos;

import org.apache.mesos.Protos;

import java.util.Collection;

public class EmbeddedMesosAgent {
    public String agentId;
    public Collection<Protos.Resource> resources;
}
