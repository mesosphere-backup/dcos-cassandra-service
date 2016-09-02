package com.mesosphere.dcos.cassandra.scheduler.plan;


import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

/**
 * EmptyBlock is an immutable singleton Block that performs no action and
 * is always complete.
 */
public class EmptyBlock implements Block {

    private static final EmptyBlock instance = new EmptyBlock();
    private static final UUID emptyId = UUID.fromString
            ("59b8f58c-7fdd-488f-9267-6fa2f77d51b7");

    public static EmptyBlock get() {
        return instance;
    }

    private EmptyBlock() {
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
    public Optional<OfferRequirement> start() {
        return Optional.empty();
    }

    @Override
    public void update(Protos.TaskStatus status) {
        // no-op
    }

    @Override
    public UUID getId() {
        return emptyId;
    }

    @Override
    public String getName() {
        return "EMPTY";
    }

    @Override
    public String getMessage() {
        return "No action to perform";
    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public void updateOfferStatus(Optional<Collection<Protos.Offer.Operation>> operations) {
        // no-op
    }

    @Override
    public void restart() {
        // no-op

    }

    @Override
    public void forceComplete() {
        // no-op
    }
}
