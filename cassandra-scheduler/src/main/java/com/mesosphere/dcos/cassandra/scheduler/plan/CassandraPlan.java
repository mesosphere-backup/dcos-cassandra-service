package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;

import org.apache.mesos.scheduler.plan.DefaultPlan;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.ReconciliationPhase;
import org.apache.mesos.scheduler.plan.Status;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.apache.mesos.scheduler.plan.strategy.Strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Extension of {@link DefaultPlan} which also includes any active maintenance tasks.
 *
 * TODO(nick): Use separate plan(s) for maintenance tasks, and use a plain DefaultPlan for deployment.
 * This requires use of a PlanCoordinator upstream, which in turn requires updating
 * CassandraRecoveryScheduler to work as a recovery plan, which in turn ... In any case, it'd be best
 * to wait for upstreamed maintenance task support and just switch whole-hog to that.
 */
public class CassandraPlan extends DefaultPlan {

    private final CassandraDaemonPhase deploy;
    private final List<ClusterTaskManager<?, ?>> clusterTaskManagers;

    public CassandraPlan(
            final DefaultConfigurationManager defaultConfigurationManager,
            final ReconciliationPhase reconciliation,
            final SyncDataCenterPhase syncDc,
            final CassandraDaemonPhase deploy,
            final List<ClusterTaskManager<?, ?>> clusterTaskManagers) {
        super("cassandra",
                Arrays.asList(reconciliation, syncDc, deploy),
                null /* strategy retrieval is overridden below */,
                defaultConfigurationManager.getErrors().stream()
                    .map(error -> error.getMessage())
                    .collect(Collectors.toList()));
        this.deploy = deploy;
        // Note: This ordering defines the ordering of the phases below:
        this.clusterTaskManagers = clusterTaskManagers;
        for (ClusterTaskManager<?, ?> manager: this.clusterTaskManagers) {
            manager.subscribe(this);
        }
    }

    @Override
    public List<Phase> getChildren() {
        // copy super.getChildren() rather than appending in-place:
        List<Phase> phases = new ArrayList<>();
        phases.addAll(super.getChildren()); // should contain phases: reconciliation, syncDc, and deploy
        if (clusterTaskManagers != null) { // may be null when DefaultPlan's constructor is calling getChildren()
            for (ClusterTaskManager<?, ?> manager : clusterTaskManagers) {
                phases.addAll(manager.getPhases());
            }
        }
        return phases;
    }

    /**
     * TODO(nick): Remove this custom override once PlanUtils.getStatus() checks for non-empty errors in the parent.
     */
    @Override
    public Status getStatus() {
        if (!getErrors().isEmpty()) {
            return Status.ERROR;
        }
        return super.getStatus();
    }

    /**
     * HACK: SerialStrategy is stateful, keeping track of plan phases. This plan meanwhile changes phases
     * dynamically depending on any maintenance tasks that are being performed, so we constantly recreate the
     * strategy so that it accurately reflects current plan contents. Otherwise it will ignore e.g. any
     * newly-started maintenance tasks.
     *
     * TODO(nick): Switch to upstreamed maintenance operation support once that's available.
     */
    @Override
    public Strategy<Phase> getStrategy() {
        return new SerialStrategy<>();
    }

    @Override
    public boolean isComplete() {
        if (!deploy.isComplete()) {
            return false;
        }
        for (ClusterTaskManager<?, ?> manager : clusterTaskManagers) {
            if (manager.isInProgress()) {
                return false;
            }
        }
        return true;
    }
}
