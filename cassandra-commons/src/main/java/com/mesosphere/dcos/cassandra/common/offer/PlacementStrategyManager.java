package com.mesosphere.dcos.cassandra.common.offer;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.constrain.PlacementRule;
import org.apache.mesos.offer.constrain.PlacementUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class PlacementStrategyManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PlacementStrategyManager.class);

    public static Optional<PlacementRule> getPlacementStrategy(
            Protos.TaskInfo taskInfo,
            DefaultConfigurationManager configurationManager,
            CassandraState cassandraState) throws ConfigStoreException {
        String placementStrategy = StringUtils.upperCase(
                ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getPlacementStrategy());

        LOGGER.info("Using placement strategy: {}", placementStrategy);

        switch (placementStrategy) {
            case "ANY":
                LOGGER.info("Returning ANY strategy");
                return Optional.empty();
            case "NODE":
            {
                LOGGER.info("Returning NODE strategy");
                NodePlacementStrategy nodePlacementStrategy = new NodePlacementStrategy(cassandraState);
                List<String> avoidAgents = nodePlacementStrategy.getAgentsToAvoid(taskInfo);
                List<String> colocateAgents = nodePlacementStrategy.getAgentsToColocate(taskInfo);
                LOGGER.info("Avoiding agents: {}", avoidAgents);
                LOGGER.info("Colocating with agents: {}", colocateAgents);
                return PlacementUtils.getAgentPlacementRule(avoidAgents, colocateAgents);
            }
            default:
                LOGGER.info("Returning DEFAULT strategy");
                return Optional.empty();
        }
    }
}
