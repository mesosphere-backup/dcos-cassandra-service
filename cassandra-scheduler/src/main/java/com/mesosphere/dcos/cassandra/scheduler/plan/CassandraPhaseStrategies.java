package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.mesos.scheduler.plan.DefaultInstallStrategy;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.PhaseStrategy;

/**
 * Created by kowens on 2/25/16.
 */
public class CassandraPhaseStrategies {

    private final Class<?> phaseStrategy;
    @Inject
    public CassandraPhaseStrategies(
            @Named("ConfiguredPhaseStrategy")String phaseStrategy) {
        try {
            this.phaseStrategy =
                    this.getClass().getClassLoader().loadClass(phaseStrategy);
        } catch (ClassNotFoundException e) {
           throw new RuntimeException(String.format(
                   "Failed to load class for phaseStrategy $s",
                   phaseStrategy
           ),e);
        }
    };

    public PhaseStrategy get(Phase phase) {
        if (phase instanceof EmptyPlan.EmptyPhase) {
            return EmptyPlan.EmptyStrategy.get();
        } else if (phase instanceof ReconciliationPhase) {
            return ReconciliationStrategy.create((ReconciliationPhase) phase);
        } else {
            try {
                return (PhaseStrategy)
                        phaseStrategy.getConstructor(Phase.class).newInstance(
                        phase);
            } catch (Exception ex){
                throw new RuntimeException("Failed to PhaseStrategy",ex);
            }
        }
    }
}
