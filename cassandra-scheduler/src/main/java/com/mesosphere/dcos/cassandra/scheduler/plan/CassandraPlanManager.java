package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.scheduler.plan.DefaultPlanManager;
import org.apache.mesos.scheduler.plan.PhaseStrategyFactory;

public class CassandraPlanManager extends DefaultPlanManager {
    @Inject
    public CassandraPlanManager(
            final PhaseStrategyFactory strategyFactory) {
        super(EmptyStage.get(), strategyFactory);
    }

    @Override
    public void update(Protos.TaskStatus status) {
        super.update(status);

        CassandraPlan cassandraPlan = (CassandraPlan) getPlan();
        cassandraPlan.update();
    }
}
