package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.common.collect.Lists;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.PlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClusterTaskPlacementStrategy implements PlacementStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterTaskPlacementStrategy.class);

    private CassandraTasks cassandraTasks;

    public ClusterTaskPlacementStrategy(CassandraTasks cassandraTasks) {
        this.cassandraTasks = cassandraTasks;
    }

    @Override
    public List<Protos.SlaveID> getAgentsToAvoid(Protos.TaskInfo taskInfo) {
        return Lists.newArrayList();
    }

    @Override
    public List<Protos.SlaveID> getAgentsToColocate(Protos.TaskInfo taskInfo) {
        List<Protos.SlaveID> agentsToColocate = new ArrayList<>();
        // Colocate this task with the corresponding Cassandra node task
        Arrays.asList(taskInfo.getSlaveId());
        LOGGER.info("Colocating task: {} with agent: {}",
                taskInfo.getTaskId().getValue(), agentsToColocate);
        return agentsToColocate;
    }
}
