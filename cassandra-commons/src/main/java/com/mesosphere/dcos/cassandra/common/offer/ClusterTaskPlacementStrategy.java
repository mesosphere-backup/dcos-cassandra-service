package com.mesosphere.dcos.cassandra.common.offer;

import com.google.common.collect.Lists;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClusterTaskPlacementStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterTaskPlacementStrategy.class);

    public static List<String> getAgentsToAvoid(Protos.TaskInfo taskInfo) {
        return Lists.newArrayList();
    }

    public static List<String> getAgentsToColocate(Protos.TaskInfo taskInfo) {
        List<String> agentsToColocate = new ArrayList<>();
        // Collocate this task with the corresponding Cassandra node task
        Arrays.asList(taskInfo.getSlaveId().getValue());
        LOGGER.info("Collocating task: {} with agent: {}",
                taskInfo.getTaskId().getValue(), agentsToColocate);
        return agentsToColocate;
    }
}
