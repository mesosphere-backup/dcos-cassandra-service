package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.DefaultStep;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class SyncDataCenterStep extends DefaultStep implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncDataCenterStep.class);

    private final String url;
    private final SeedsManager seeds;
    private final ExecutorService executor;

    private Optional<DataCenterInfo> byUrl() {
        return seeds.getDataCenters().stream()
                .filter(dataCenter -> dataCenter.getUrl().equalsIgnoreCase(url))
                .findFirst();
    }

    public static SyncDataCenterStep create(String url,
                                             SeedsManager seeds,
                                             ExecutorService executor){
        return new SyncDataCenterStep(url,seeds,executor);
    }

    public SyncDataCenterStep(final String url,
                               final SeedsManager seeds,
                               final ExecutorService executor) {
        super("Sync DataCenter", Optional.empty(), Status.PENDING, Collections.emptyList());
        this.url = url;
        this.seeds = seeds;
        this.executor = executor;
    }

    @Override
    public Optional<OfferRequirement> start() {
        Optional<DataCenterInfo> dc = byUrl();
        if (dc.isPresent() && dc.get().getSeeds().size() > 0) {
            LOGGER.info("Block {} : Data center synced {}", getName(), url);
            setStatus(Status.COMPLETE);
        } else {
            LOGGER.info("Block {} : Syncing data center {}", getName(), url);
            setStatus(Status.IN_PROGRESS);
            executor.execute(this);
        }
        return Optional.empty();
    }

    @Override
    public String getMessage() {
        // Append additional info to message
        return String.format("%s Syncing data center @ %s", super.getMessage(), url);
    }

    @Override
    public void run() {
        while (!isComplete()) {
            if (seeds.sync(url)) {
                setStatus(Status.COMPLETE);
            }
        }
    }
}
