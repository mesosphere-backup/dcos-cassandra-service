package com.mesosphere.dcos.cassandra.scheduler.plan;


import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SyncDataCenterPhase extends DefaultPhase {

    private static List<Step> createSteps(
            SeedsManager seeds,
            ExecutorService executor) throws ConfigStoreException {
        Map<String, DataCenterInfo> synched =
                seeds.getDataCenters().stream()
                        .filter(dc -> dc.getSeeds().size() > 0)
                        .collect(Collectors.toMap(
                                DataCenterInfo::getUrl,
                                Function.identity()));

        return seeds.getConfiguredDataCenters().stream()
                .filter(url -> !synched.containsKey(url))
                .map(url -> SyncDataCenterStep.create(url, seeds, executor))
                .collect(Collectors.toList());
    }


    public static SyncDataCenterPhase create(SeedsManager seeds,
                                             ExecutorService executor) throws ConfigStoreException {
        return new SyncDataCenterPhase(createSteps(seeds, executor));
    }

    private SyncDataCenterPhase(List<Step> steps) {
        super("Sync Datacenter", steps, new SerialStrategy<>(), Collections.emptyList());
    }
}
