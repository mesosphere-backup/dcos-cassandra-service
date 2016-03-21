package com.mesosphere.dcos.cassandra.scheduler.plan;


import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.DefaultPhase;

import java.util.Collections;
import java.util.UUID;

/**
 * EmptyPhase is an immutable singleton Phase that contains the EmptyBlock.
 */
public class EmptyPhase extends DefaultPhase {
    public static final UUID EMPTY_PHASE_ID =
            UUID.fromString("50b09c45-afc8-4af1-8c23-1002ce5a01f6");
    private static final EmptyPhase instance = new EmptyPhase();

    public static EmptyPhase get() {
        return instance;
    }

    private EmptyPhase() {
        super(EMPTY_PHASE_ID, "EmptyPhase", Collections.<Block>emptyList());
    }
}
