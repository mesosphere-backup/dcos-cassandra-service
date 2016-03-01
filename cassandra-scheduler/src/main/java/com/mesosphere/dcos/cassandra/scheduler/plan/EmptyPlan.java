package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.*;

import java.util.Collections;
import java.util.List;

public class EmptyPlan implements Plan {

    public static class EmptyBlock implements Block {

        private static final EmptyBlock instance = new EmptyBlock();

        public static EmptyBlock get() {
            return instance;
        }

        private EmptyBlock() {
        }

        @Override
        public Status getStatus() {
            return Status.Complete;
        }

        @Override
        public void setStatus(Status newStatus) {

        }

        @Override
        public boolean isPending() {
            return false;
        }

        @Override
        public boolean isInProgress() {
            return false;
        }

        @Override
        public OfferRequirement start() {
            return null;
        }

        @Override
        public void update(Protos.TaskStatus status) {

        }

        @Override
        public int getId() {
            return Integer.MIN_VALUE;
        }

        @Override
        public String getName() {
            return "EMPTY";
        }

        @Override
        public boolean isComplete() {
            return true;
        }
    }

    public static class EmptyPhase implements Phase{
        private static final EmptyPhase instance = new EmptyPhase();

        public static EmptyPhase get(){
            return instance;
        }

        private EmptyPhase(){}
        @Override
        public List<? extends Block> getBlocks() {
            return Collections.EMPTY_LIST;
        }


        @Override
        public int getId() {
            return Integer.MIN_VALUE;
        }

        @Override
        public String getName() {
            return "EMPTY";
        }

        @Override
        public boolean isComplete() {
            return true;
        }
    }

    public static class EmptyStrategy implements PhaseStrategy {

        private static final EmptyStrategy instance = new EmptyStrategy();

        public static EmptyStrategy get(){
            return instance;
        }
        private final EmptyPhase phase = EmptyPhase.get();
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

    private static final EmptyPlan instance = new EmptyPlan();
    public static EmptyPlan get(){
        return instance;
    }

    private EmptyPlan() {}
    @Override
    public List<? extends Phase> getPhases() {
        return Collections.emptyList();
    }

    @Override
    public boolean isComplete() {
        return true;
    }
}
