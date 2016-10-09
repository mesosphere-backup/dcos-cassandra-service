package com.mesosphere.dcos.cassandra.common.offer;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.constrain.AgentRule;
import org.apache.mesos.offer.constrain.AndRule;
import org.apache.mesos.offer.constrain.PlacementRuleGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class PlacementStrategyManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PlacementStrategyManager.class);

    public static Optional<PlacementRuleGenerator> getPlacementStrategy(
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
                LOGGER.info("Returning NODE strategy");
                NodePlacementStrategy nodePlacementStrategy = new NodePlacementStrategy(cassandraState);
                return getPlacement(
                        nodePlacementStrategy.getAgentsToAvoid(taskInfo),
                        nodePlacementStrategy.getAgentsToColocate(taskInfo));
            default:
                LOGGER.info("Returning DEFAULT strategy");
                return Optional.empty();
        }
    }

    public static Optional<PlacementRuleGenerator> getPlacement(List<String> avoidAgents, List<String> colocateAgents) {
        LOGGER.info("Avoiding agents: {}", avoidAgents);
        LOGGER.info("Colocating with agents: {}", colocateAgents);

        Optional<PlacementRuleGenerator> placement;
        if (!avoidAgents.isEmpty()) {
            if (!colocateAgents.isEmpty()) {
                // avoid and colocate enforcement
                placement = Optional.of(new AndRule.Generator(
                        new AgentRule.AvoidAgentsGenerator(avoidAgents),
                        new AgentRule.ColocateAgentsGenerator(colocateAgents)));
            } else {
                // avoid enforcement only
                placement = Optional.of(new AgentRule.AvoidAgentsGenerator(avoidAgents));
            }
        } else if (!colocateAgents.isEmpty()) {
            // colocate enforcement only
            placement = Optional.of(new AgentRule.ColocateAgentsGenerator(colocateAgents));
        } else {
            // no colocate/avoid enforcement
            placement = Optional.empty();
        }

        return placement;
    }
}
