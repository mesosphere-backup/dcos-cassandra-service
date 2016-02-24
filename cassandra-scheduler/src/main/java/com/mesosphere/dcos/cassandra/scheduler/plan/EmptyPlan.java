package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Collections;
import java.util.List;

public class EmptyPlan implements Plan {

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
    public Phase getCurrentPhase() {
        return EmptyPhase.get();
    }

    @Override
    public Status getStatus() {
        return Status.Complete;
    }

    @Override
    public boolean isComplete() {
        return true;
    }
}
