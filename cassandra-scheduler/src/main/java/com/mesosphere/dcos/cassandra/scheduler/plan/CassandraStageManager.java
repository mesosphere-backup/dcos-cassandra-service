package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.inject.Inject;
import org.apache.mesos.scheduler.plan.DefaultStageManager;
import org.apache.mesos.scheduler.plan.EmptyStage;
import org.apache.mesos.scheduler.plan.PhaseStrategyFactory;

/**
 * Created by kowens on 3/7/16.
 */
public class CassandraStageManager extends DefaultStageManager {
    @Inject
    public CassandraStageManager(PhaseStrategyFactory strategyFactory) {
        super(EmptyStage.get(), strategyFactory);
    }
}
