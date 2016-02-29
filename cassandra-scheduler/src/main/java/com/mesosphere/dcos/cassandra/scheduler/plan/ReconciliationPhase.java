package com.mesosphere.dcos.cassandra.scheduler.plan;


import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.tasks.Reconciler;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ReconciliationPhase implements Phase {

    private static class ReconciliationBlock implements Block {

        public static final ReconciliationBlock create(Reconciler reconciler){
            return new ReconciliationBlock(reconciler);
        }

        private final Reconciler reconciler;

        private ReconciliationBlock(final Reconciler reconciler){
            this.reconciler = reconciler;
        }

        @Override
        public Status getStatus() {
            return (reconciler.isReconciled()) ?
                    Status.Complete : Status.InProgress;
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
            return !isComplete();
        }

        @Override
        public OfferRequirement start() {
            return null;
        }

        @Override
        public void update(Protos.TaskStatus status) {
            reconciler.update(status);
        }

        @Override
        public List<Protos.TaskID> getUpdateIds() {
            return Collections.emptyList();
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        public String getName() {
            return "RECONCILER";
        }

        @Override
        public boolean isComplete() {
            return reconciler.isReconciled();
        }

        public Reconciler getReconciler(){
            return reconciler;
        }
    }

    public static final ReconciliationPhase create(
            final Reconciler reconciler){
        return new ReconciliationPhase(reconciler);
    }

    private final ReconciliationBlock block;
    private final List<Block> blocks;
    ReconciliationPhase(final Reconciler reconciler){
        block = ReconciliationBlock.create(reconciler);
        blocks = Arrays.asList(block);
    }

    @Override
    public List<? extends Block> getBlocks() {
        return blocks;
    }

    @Override
    public int getId() {
        return block.getId();
    }

    @Override
    public String getName() {
        return "RECONCILIATION";
    }

    @Override
    public boolean isComplete() {
        return block.isComplete();
    }
}
