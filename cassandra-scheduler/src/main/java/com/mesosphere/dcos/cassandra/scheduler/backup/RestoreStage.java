package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Stage;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RestoreStage implements Stage {
    private DownloadSnapshotPhase downloadSnapshotPhase;
    private RestoreSnapshotPhase restoreSnapshotPhase;

    public RestoreStage(RestoreContext restoreContext,
                        CassandraTasks cassandraTasks,
                        ClusterTaskOfferRequirementProvider provider) {
        this.downloadSnapshotPhase = new DownloadSnapshotPhase(restoreContext,
                cassandraTasks, provider);
        this.restoreSnapshotPhase = new RestoreSnapshotPhase(restoreContext,
                cassandraTasks, provider);
    }

    @Override
    public List<? extends Phase> getPhases() {
        return Arrays.asList(downloadSnapshotPhase, restoreSnapshotPhase);
    }

    @Override
    public List<String> getErrors() {
        return Collections.emptyList();
    }

    @Override
    public boolean isComplete() {
        return restoreSnapshotPhase.isComplete();
    }
}
