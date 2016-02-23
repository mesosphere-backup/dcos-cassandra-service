package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Collections;
import java.util.List;

public class EmptyBlock implements Block {

    private static final EmptyBlock instance = new EmptyBlock();

    public static EmptyBlock get(){
        return instance;
    }

    private EmptyBlock(){}
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
    public List<Protos.TaskID> getUpdateIds() {
        return Collections.emptyList();
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
