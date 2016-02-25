package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.PhaseStrategy;


public class ReconciliationStrategy implements PhaseStrategy {

    public static ReconciliationStrategy create(final ReconciliationPhase
                                                        phase){
        return new ReconciliationStrategy(phase);
    }

    private final ReconciliationPhase phase;

    public ReconciliationStrategy(final ReconciliationPhase phase){
        this.phase = phase;
    }
    @Override
    public Block getCurrentBlock() {
        return phase.getBlocks().get(0);
    }

    @Override
    public void proceed() {

    }

    @Override
    public void interrupt() {

    }

    @Override
    public void restart(int blockIndex, boolean force)
            throws IndexOutOfBoundsException {

    }

    @Override
    public Phase getPhase() {
        return phase;
    }
}
