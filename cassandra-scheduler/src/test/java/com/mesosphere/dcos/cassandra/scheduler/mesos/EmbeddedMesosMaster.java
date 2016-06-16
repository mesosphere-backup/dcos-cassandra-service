package com.mesosphere.dcos.cassandra.scheduler.mesos;

import java.util.Collection;

public class EmbeddedMesosMaster {
    public String id;
    int ip;
    public int port;
    public Collection<EmbeddedMesosAgent> registeredAgents;
}
