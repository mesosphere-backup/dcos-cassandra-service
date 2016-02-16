package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.offer.AnyPlacementStrategy;
import org.apache.mesos.offer.PlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlacementStrategyManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PlacementStrategyManager.class);

    public static PlacementStrategy getPlacementStrategy(
            ConfigurationManager configurationManager,
            CassandraTasks cassandraTasks) {
        String placementStrategy = StringUtils.upperCase(
                configurationManager.getPlacementStrategy());

        LOGGER.info("Using placement strategy: {}", placementStrategy);

        switch (placementStrategy) {
            case "ANY":
                LOGGER.info("Returning ANY strategy");
                return new AnyPlacementStrategy();
            case "NODE":
                LOGGER.info("Returning NODE strategy");
                return new NodePlacementStrategy(cassandraTasks);
            default:
                LOGGER.info("Returning DEFAULT strategy");
                return new AnyPlacementStrategy();
        }
    }
}
