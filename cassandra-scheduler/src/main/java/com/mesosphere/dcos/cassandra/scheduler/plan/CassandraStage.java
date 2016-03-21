package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Stage;

import java.util.List;

public class CassandraStage implements Stage {


    public static final CassandraStage create(
            final ConfigurationManager configuration,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair) {

        return new CassandraStage(
                configuration,
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
    private final ConfigurationManager configuration;
    private final CleanupManager cleanup;
    private final RepairManager repair;

    public CassandraStage(
            final ConfigurationManager configuration,
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair) {

        this.configuration = configuration;
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
                .addAll(configuration.getErrors())
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
}
