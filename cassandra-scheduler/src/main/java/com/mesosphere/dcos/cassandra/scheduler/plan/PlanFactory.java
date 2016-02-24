package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

public class PlanFactory {

    private PlanFactory() {
    }

    public static CassandraDeploy getPlan(
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final ConfigurationManager configurationManager,
            final EventBus eventBus,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {
        return new CassandraDeploy(
                offerRequirementProvider,
                configurationManager,
                eventBus,
                cassandraTasks,
                client);
    }
}
