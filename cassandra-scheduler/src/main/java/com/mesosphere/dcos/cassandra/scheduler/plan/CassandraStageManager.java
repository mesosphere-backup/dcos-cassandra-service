package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.scheduler.plan.DefaultStageManager;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.PhaseStrategyFactory;

public class CassandraStageManager extends DefaultStageManager {
    @Inject
    public CassandraStageManager(
            final PhaseStrategyFactory strategyFactory) {
        super(EmptyStage.get(), strategyFactory);
    }

    @Override
    public Phase getCurrentPhase() {
        return super.getCurrentPhase();
    }

    @Override
    public void update(Protos.TaskStatus status) {
        super.update(status);

        CassandraStage cassandraStage = (CassandraStage) getStage();
        cassandraStage.update();
    }
}
