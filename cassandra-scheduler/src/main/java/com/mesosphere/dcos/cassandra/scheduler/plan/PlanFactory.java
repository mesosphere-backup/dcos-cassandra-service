package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

public class PlanFactory {

    private PlanFactory() {
    }

    public static CassandraPlan getPlan(
            CassandraOfferRequirementProvider offerRequirementProvider,
            ConfigurationManager configurationManager, EventBus eventBus, CassandraTasks cassandraTasks) {
        return new CassandraPlan(offerRequirementProvider,
                configurationManager, eventBus, cassandraTasks);
    }
}
