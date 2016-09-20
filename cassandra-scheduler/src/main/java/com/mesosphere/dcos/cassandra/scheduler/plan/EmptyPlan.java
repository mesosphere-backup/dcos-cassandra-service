package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;

import java.util.Collections;
import java.util.List;

/**
 * EmptyPlan is used to represent and a Stage that is complete and can not be
 * executed. It is useful for frameworks that must provide a Stage to clients
 * (e.g. via a REST API) prior to being able to construct Stage. This occurs,
 * for instance, when a framework scheduler is running, but has not yet
 * retrieved its persistent state and registered with the Mesos Master. The
 * EmptyPlan is an immutable singleton that contains exactly one Phase (the
 * EmptyPhase).
 */
public class EmptyPlan implements Plan {

    private static final EmptyPlan instance = new EmptyPlan();

    public static EmptyPlan get() {
        return instance;
    }

    private EmptyPlan() {
    }

    @Override
    public List<? extends Phase> getPhases() {
        return Collections.emptyList();
    }

    @Override
    public boolean isComplete() {
        return true;
    }

    public List<String> getErrors() {
        return Collections.emptyList();
    }

}
