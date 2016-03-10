package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.scheduler.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.backup.RestoreManager;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Stage;

import java.util.Collections;
import java.util.List;

public class CassandraStage implements Stage {


    public static final CassandraStage create(
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore) {

        return new CassandraStage(deployment,
                backup,
                restore
        );
    }

    private final DeploymentManager deployment;
    private final BackupManager backup;
    private final RestoreManager restore;

    public CassandraStage(
            final DeploymentManager deployment,
            final BackupManager backup,
            final RestoreManager restore) {

        this.deployment = deployment;
        this.backup = backup;
        this.restore = restore;
    }

    @Override
    public List<? extends Phase> getPhases() {

        return ImmutableList.<Phase>builder()
                .addAll(deployment.getPhases())
                .addAll(backup.getPhases())
                .addAll(restore.getPhases())
                .build();
    }

    @Override
    public List<String> getErrors() {
        return Collections.emptyList();
    }


    @Override
    public boolean isComplete() {
        return deployment.isComplete() &&
                (backup.inProgress()) ? backup.isComplete() : true &&
                (restore.inProgress()) ? restore.isComplete() : true;

    }
}
