package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Arrays;
import java.util.List;

public class RestorePlan implements Plan {
    private DownloadSnapshotPhase downloadSnapshotPhase;
    private RestoreSnapshotPhase restoreSnapshotPhase;

    public RestorePlan(RestoreContext restoreContext,
                       int servers,
                       CassandraTasks cassandraTasks,
                       EventBus eventBus,
                       ClusterTaskOfferRequirementProvider provider) {
        this.downloadSnapshotPhase = new DownloadSnapshotPhase(restoreContext, servers, cassandraTasks, eventBus, provider);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return Arrays.asList(downloadSnapshotPhase);
    }

    @Override
    public Phase getCurrentPhase() {
        return downloadSnapshotPhase;
    }

    @Override
    public Status getStatus() {
        return getCurrentPhase().getStatus();
    }

    @Override
    public boolean isComplete() {
        return downloadSnapshotPhase.isComplete();
    }
}
