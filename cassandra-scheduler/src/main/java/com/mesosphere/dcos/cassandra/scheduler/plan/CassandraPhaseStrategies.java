package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.scheduler.plan.DefaultInstallStrategy;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.PhaseStrategy;

/**
 * Created by kowens on 2/25/16.
 */
public class CassandraPhaseStrategies {

    private CassandraPhaseStrategies() {
    };

    public static PhaseStrategy get(Phase phase) {
        if (phase instanceof EmptyPlan.EmptyPhase) {
            return EmptyPlan.EmptyStrategy.get();
        } else if (phase instanceof ReconciliationPhase) {
            return ReconciliationStrategy.create((ReconciliationPhase) phase);
        } else {
            return new DefaultInstallStrategy(phase);
        }
    }
}
