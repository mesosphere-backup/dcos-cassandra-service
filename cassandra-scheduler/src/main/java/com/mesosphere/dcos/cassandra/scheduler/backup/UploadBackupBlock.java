package com.mesosphere.dcos.cassandra.scheduler.backup;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;

import java.util.List;

public class UploadBackupBlock implements Block {
    @Override
    public Status getStatus() {
        return null;
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
        return null;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isComplete() {
        return false;
    }
}
