package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.mesosphere.dcos.cassandra.common.backup.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractClusterTaskBlock<C extends ClusterTaskContext> implements Block {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AbstractClusterTaskBlock.class);

    protected int id;
    protected String taskId;
    protected Status status;
    protected C context;
    protected CassandraTasks cassandraTasks;
    protected CassandraOfferRequirementProvider provider;

    public AbstractClusterTaskBlock(int id,
                                    String taskId,
                                    CassandraTasks cassandraTasks,
                                    CassandraOfferRequirementProvider provider,
                                    C context) {
        this.id = id;
        this.taskId = taskId;
        this.provider = provider;
        this.status = Status.Pending;
        this.context = context;
        this.cassandraTasks = cassandraTasks;
    }

    public abstract String getName();

    public abstract OfferRequirement start();

    public abstract void update(Protos.TaskStatus status);

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void setStatus(Status newStatus) {
        LOGGER.info("{}: changing status from: {} to: {}", getName(), status, newStatus);
        status = newStatus;
    }

    @Override
    public boolean isPending() {
        return Status.Pending == this.status;
    }

    @Override
    public boolean isInProgress() {
        return Status.InProgress == this.status;
    }

    protected boolean isRelevantStatus(Protos.TaskStatus status) {
        return taskId.equals(status.getTaskId().getValue());
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public boolean isComplete() {
        return Status.Complete == this.status;
    }

    public String getTaskId() {
        return taskId;
    }
}
