package com.mesosphere.dcos.cassandra.executor;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gabriel on 9/20/16.
 */
public class Probe {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CassandraDaemonTask task;
    private NodeProbe nodeProbe = null;

    public Probe(CassandraDaemonTask task) {
       this.task = task;
    }

    public NodeProbe get() {
        if (nodeProbe == null) {
            nodeProbe = connectProbe();
        }

        return nodeProbe;
    }

    private NodeProbe connectProbe() {
        while (true) {
            try {
                NodeProbe nodeProbe = new NodeProbe("127.0.0.1", task.getConfig().getJmxPort());
                logger.info("Node probe is successfully connected to the Cassandra Daemon: port {}",
                        task.getConfig().getJmxPort());
                return nodeProbe;
            } catch (Exception ex) {
                logger.info("Connection to server failed backing off for 500 ms");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }
        }
    }
}
