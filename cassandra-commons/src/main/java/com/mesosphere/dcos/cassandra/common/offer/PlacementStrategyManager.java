package com.mesosphere.dcos.cassandra.common.offer;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.AnyPlacementStrategy;
import org.apache.mesos.offer.PlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlacementStrategyManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PlacementStrategyManager.class);

    public static PlacementStrategy getPlacementStrategy(
            DefaultConfigurationManager configurationManager,
            CassandraState cassandraState) throws ConfigStoreException {
        String placementStrategy = StringUtils.upperCase(
                ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getPlacementStrategy());

        LOGGER.info("Using placement strategy: {}", placementStrategy);

        switch (placementStrategy) {
            case "ANY":
                LOGGER.info("Returning ANY strategy");
                return new AnyPlacementStrategy();
            case "NODE":
                LOGGER.info("Returning NODE strategy");
                return new NodePlacementStrategy(cassandraState);
            default:
                LOGGER.info("Returning DEFAULT strategy");
                return new AnyPlacementStrategy();
        }
    }
}
