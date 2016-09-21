package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.scheduler.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Plan;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraPlan implements Plan {

    public static final CassandraPlan create(
            final DefaultConfigurationManager defaultConfigurationManager,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair) {

        return new CassandraPlan(
                defaultConfigurationManager,
                deployment,
                backup,
                restore,
                cleanup,
                repair
        );
    }

    private final DefaultConfigurationManager defaultConfigurationManager;
    private final DeploymentManager deployment;
    private final List<ClusterTaskManager<?>> managers;

    public CassandraPlan(
            final DefaultConfigurationManager defaultConfigurationManager,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair) {
        this.defaultConfigurationManager = defaultConfigurationManager;
        this.deployment = deployment;
        // Note: This ordering defines the ordering of the phases below:
        this.managers = Arrays.asList(backup, restore, cleanup, repair);
    }

    @Override
    public List<? extends Phase> getPhases() {
        ImmutableList.Builder<Phase> builder =
                ImmutableList.<Phase>builder().addAll(deployment.getPhases());
        for (ClusterTaskManager<?> manager : managers) {
            builder.addAll(manager.getPhases());
        }
        return builder.build();
    }

    @Override
    public List<String> getErrors() {
        return ImmutableList.<String>builder()
                .addAll(defaultConfigurationManager
                        .getErrors()
                        .stream()
                        .map(error -> error.getMessage())
                        .collect(Collectors.toList()))
                .addAll(deployment.getErrors())
                .build();
    }


    @Override
    public boolean isComplete() {
        if (!deployment.isComplete()) {
            return false;
        }
        for (ClusterTaskManager<?> manager : managers) {
            if (manager.isInProgress() && !manager.isComplete()) {
                return false;
            }
        }
        return true;

    }

    public void update() {
        for (ClusterTaskManager<?> manager : managers) {
            if (manager.isComplete()) {
                manager.stop();
            }
        }
    }
}
