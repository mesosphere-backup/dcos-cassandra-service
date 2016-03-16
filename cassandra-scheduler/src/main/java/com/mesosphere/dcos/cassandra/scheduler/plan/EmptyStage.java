package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Stage;

import java.util.Collections;
import java.util.List;

/**
 * EmptyStage is used to represent and a Stage that is complete and can not be
 * executed. It is useful for frameworks that must provide a Stage to clients
 * (e.g. via a REST API) prior to being able to construct Stage. This occurs,
 * for instance, when a framework scheduler is running, but has not yet
 * retrieved its persistent state and registered with the Mesos Master. The
 * EmptyPlan is an immutable singleton that contains exactly one Phase (the
 * EmptyPhase).
 */
public class EmptyStage implements Stage {

    private static final EmptyStage instance = new EmptyStage();

    public static EmptyStage get() {
        return instance;
    }

    private EmptyStage() {
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
