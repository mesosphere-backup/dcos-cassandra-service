package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.PhaseStrategy;
import org.apache.mesos.scheduler.plan.Status;

import java.util.UUID;

/**
 * A strategy that does nothing.
 */
public class NoOpPhaseStrategy implements PhaseStrategy {
    private static final NoOpPhaseStrategy instance = new NoOpPhaseStrategy();

    public static NoOpPhaseStrategy get() {
        return instance;
    }

    private NoOpPhaseStrategy() {
    }

    @Override
    public Block getCurrentBlock() {
        return EmptyBlock.get();
    }

    @Override
    public void proceed() {

    }

    @Override
    public void interrupt() {

    }

    @Override
    public void restart(UUID blockId) {

    }

    @Override
    public void forceComplete(UUID blockId) {

    }

    @Override
    public Status getStatus() {
        return Status.Complete;
    }


    @Override
    public Phase getPhase() {
        return EmptyPhase.get();
    }

    @Override
    public boolean isInterrupted() {
        return false;
    }

    @Override
    public boolean hasDecisionPoint(Block block) {
        return false;
    }
}
