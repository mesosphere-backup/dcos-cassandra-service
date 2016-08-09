package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class CassandraStage implements Stage {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    CassandraStage.class);

    public static final CassandraStage create(
            final DefaultConfigurationManager defaultConfigurationManager,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair) {

        return new CassandraStage(
                defaultConfigurationManager,
                deployment,
                backup,
                restore,
                cleanup,
                repair
        );
    }

    private final DeploymentManager deployment;
    private final BackupManager backup;
    private final RestoreManager restore;
    private final DefaultConfigurationManager defaultConfigurationManager;
    private final CleanupManager cleanup;
    private final RepairManager repair;

    public CassandraStage(
            final DefaultConfigurationManager defaultConfigurationManager,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair) {
        this.defaultConfigurationManager = defaultConfigurationManager;
        this.deployment = deployment;
        this.backup = backup;
        this.restore = restore;
        this.cleanup = cleanup;
        this.repair = repair;
    }

    @Override
    public List<? extends Phase> getPhases() {
        return ImmutableList.<Phase>builder()
                .addAll(deployment.getPhases())
                .addAll(backup.getPhases())
                .addAll(cleanup.getPhases())
                .addAll(restore.getPhases())
                .addAll(repair.getPhases())
                .build();
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
        return deployment.isComplete() &&
                (backup.inProgress() ? backup.isComplete() : true) &&
                (restore.inProgress() ? restore.isComplete() : true) &&
                (cleanup.inProgress() ? cleanup.isComplete() : true) &&
                (repair.inProgress() ? repair.isComplete() : true);

    }

    public void update() {
        if (backup.isComplete()) {
            backup.stopBackup();
        }

        if (restore.isComplete()) {
            restore.stopRestore();
        }

        if (cleanup.isComplete()) {
            cleanup.stopCleanup();
        }

        if (repair.isComplete()) {
            repair.stopRepair();
        }
    }
}
