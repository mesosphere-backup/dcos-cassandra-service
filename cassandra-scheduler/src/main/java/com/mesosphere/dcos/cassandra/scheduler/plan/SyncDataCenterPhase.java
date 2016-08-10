package com.mesosphere.dcos.cassandra.scheduler.plan;


import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.scheduler.plan.DefaultPhase;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SyncDataCenterPhase extends DefaultPhase {

    private static List<SyncDataCenterBlock> createBlocks(
            SeedsManager seeds,
            ExecutorService executor
    ) throws ConfigStoreException {
        Map<String, DataCenterInfo> synched =
                seeds.getDataCenters().stream()
                        .filter(dc -> dc.getSeeds().size() > 0)
                        .collect(Collectors.toMap(
                                DataCenterInfo::getUrl,
                                Function.identity()));

        return seeds.getConfiguredDataCenters().stream()
                .filter(url -> !synched.containsKey(url))
                .map(url -> SyncDataCenterBlock.create(url, seeds, executor))
                .collect(Collectors.toList());
    }


    public static SyncDataCenterPhase create(SeedsManager seeds,
                                             ExecutorService executor) throws ConfigStoreException {
        return new SyncDataCenterPhase(createBlocks(seeds, executor));
    }

    public SyncDataCenterPhase(List<SyncDataCenterBlock> blocks) {
        super(UUID.randomUUID(), "Sync Datacenter", blocks);
    }
}
