package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class SyncDataCenterBlock implements Block, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SyncDataCenterBlock.class
    );
    private volatile Status status = Status.Pending;
    private final String url;
    private final SeedsManager seeds;
    private final UUID id = UUID.randomUUID();
    private final ExecutorService executor;

    private Optional<DataCenterInfo> byUrl() {
        return seeds.getDataCenters().stream()
                .filter(dataCenter -> dataCenter.getUrl().equalsIgnoreCase(url))
                .findFirst();
    }

    public static SyncDataCenterBlock create(String url,
                                             SeedsManager seeds,
                                             ExecutorService executor){
        return new SyncDataCenterBlock(url,seeds,executor);
    }

    public SyncDataCenterBlock(final String url,
                               final SeedsManager seeds,
                               final ExecutorService executor) {
        this.url = url;
        this.seeds = seeds;
        this.executor = executor;
    }

    @Override
    public boolean isPending() {
        return status == Status.Pending;
    }

    @Override
    public boolean isInProgress() {
        return status == Status.InProgress;
    }

    @Override
    public boolean isComplete() {
        return status == Status.Complete;
    }

    @Override
    public OfferRequirement start() {

        Optional<DataCenterInfo> dc = byUrl();
        if (dc.isPresent() && dc.get().getSeeds().size() > 0) {
            LOGGER.info("Block {} : Data center synced {}", getName(),
                    url);
            setStatus(Status.Complete);
        }
        setStatus(Status.InProgress);
        executor.execute(this);
        return null;
    }

    @Override
    public void update(Protos.TaskStatus status) {

    }

    @Override
    public void updateOfferStatus(boolean accepted) {
        // Not expected to be called: start() always returns a null OfferRequirement.
    }

    @Override
    public void restart() {
        //TODO(nick): Any additional actions to perform when restarting work?
        setStatus(Status.Pending);
    }

    @Override
    public void forceComplete() {
        //TODO(nick): Any additional actions to perform when forcing complete?
        setStatus(Status.Complete);
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public String getName() {
        return "Sync DataCenter";
    }

    @Override
    public String getMessage() {
        return "Syncing data center @ " + url;
    }

    @Override
    public void run() {
        while (!isComplete()) {
            if (seeds.sync(url)) {
                setStatus(Status.Complete);
            }
        }
    }

    private void setStatus(Status newStatus) {
        LOGGER.info("Block {} setting status to {}", getName(), newStatus);
        status = newStatus;
    }
}
