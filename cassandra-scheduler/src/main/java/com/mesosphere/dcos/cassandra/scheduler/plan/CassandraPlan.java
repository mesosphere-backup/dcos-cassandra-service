package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable.UpgradeSSTableManager;

import org.apache.mesos.scheduler.plan.DefaultPlan;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraPlan extends DefaultPlan {

    public static final CassandraPlan create(
            final DefaultConfigurationManager defaultConfigurationManager,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair,
            final UpgradeSSTableManager upgrade) {

        return new CassandraPlan(
                defaultConfigurationManager,
                deployment,
                backup,
                restore,
                cleanup,
                repair,
                upgrade
        );
    }

    private final DeploymentManager deployment;
    private final List<ClusterTaskManager<?, ?>> clusterTaskManagers;

    public CassandraPlan(
            final DefaultConfigurationManager defaultConfigurationManager,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair,
            final UpgradeSSTableManager upgrade) {
        super("cassandra",
                new ArrayList<>(), // phases overridden by getChildren() below
                new SerialStrategy<>(),
                defaultConfigurationManager.getErrors().stream()
                    .map(error -> error.getMessage())
                    .collect(Collectors.toList()));
        this.deployment = deployment;
        // Note: This ordering defines the ordering of the phases below:
        this.clusterTaskManagers = Arrays.asList(backup, restore, cleanup, repair, upgrade);

        this.deployment.subscribe(this);
        for (ClusterTaskManager<?, ?> manager: this.clusterTaskManagers) {
            manager.subscribe(this);
        }
    }

    @Override
    public List<Phase> getChildren() {
        ImmutableList.Builder<Phase> builder =
                ImmutableList.<Phase>builder().addAll(deployment.getPhases());
        for (ClusterTaskManager<?, ?> manager : clusterTaskManagers) {
            builder.addAll(manager.getPhases());
        }
        return builder.build();
    }

    @Override
    public boolean isComplete() {
        if (!deployment.isComplete()) {
            return false;
        }
        for (ClusterTaskManager<?, ?> manager : clusterTaskManagers) {
            if (manager.isInProgress() && !manager.isComplete()) {
                return false;
            }
        }
        return true;
    }

    public void update() {
        for (ClusterTaskManager<?, ?> manager : clusterTaskManagers) {
            if (manager.isComplete()) {
                manager.stop();
            }
        }
    }
}
