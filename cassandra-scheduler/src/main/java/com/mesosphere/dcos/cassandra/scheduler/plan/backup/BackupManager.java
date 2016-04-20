package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentReference;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Phase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * BackupManager is responsible for orchestrating cluster-wide backup.
 * It also ensures that only one backup can run an anytime. For each new backup
 * a new BackupPlan is created, which will assist in orchestration.
 */
public class BackupManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupManager.class);
    public static final String BACKUP_KEY = "backup";

    private final CassandraTasks cassandraTasks;
    private final ClusterTaskOfferRequirementProvider provider;
    private final PersistentReference<BackupContext> persistentBackupContext;
    private volatile BackupSnapshotPhase backup = null;
    private volatile UploadBackupPhase upload = null;
    private volatile BackupContext backupContext = null;

    @Inject
    public BackupManager(
            CassandraTasks cassandraTasks,
            ClusterTaskOfferRequirementProvider provider,
            PersistenceFactory persistenceFactory,
            final Serializer<BackupContext> serializer) {
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;

        // Load BackupManager from state store
        this.persistentBackupContext = persistenceFactory.createReference(
                BACKUP_KEY, serializer);
        try {
            final Optional<BackupContext> loadedBackupContext = persistentBackupContext.load();
            if (loadedBackupContext.isPresent()) {
                BackupContext backupContext = loadedBackupContext.get();
                // Recovering from failure
                if (backupContext != null) {
                    this.backup = new BackupSnapshotPhase(
                            backupContext,
                            cassandraTasks,
                            provider);
                    this.upload = new UploadBackupPhase(
                            backupContext,
                            cassandraTasks,
                            provider);
                    this.backupContext = backupContext;
                }
            }

        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error loading backup context from peristence store. Reason: ",
                    e);
            throw new RuntimeException(e);
        }
    }


    public void startBackup(BackupContext context) {
        LOGGER.info("Starting backup");

        if (canStartBackup()) {
            try {
                if (isComplete()) {
                    for (String name : cassandraTasks.getBackupSnapshotTasks().keySet()) {
                        cassandraTasks.remove(name);
                    }
                    for (String name : cassandraTasks.getBackupUploadTasks().keySet()) {
                        cassandraTasks.remove(name);
                    }
                }
                persistentBackupContext.store(context);
                this.backup = new BackupSnapshotPhase(
                        context,
                        cassandraTasks,
                        provider);
                this.upload = new UploadBackupPhase(
                        context,
                        cassandraTasks,
                        provider);
                backupContext = context;
            } catch (PersistenceException e) {
                LOGGER.error(
                        "Error storing backup context into persistence store. Reason: ",
                        e);

            }
        }
    }

    public void stopBackup() {
        LOGGER.info("Stopping backup");
        try {
            this.persistentBackupContext.delete();
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting backup context from persistence store. Reason: {}",
                    e);
        }
        this.backupContext = null;
    }

    public boolean canStartBackup() {
        // If backupContext is null, then we can start backup; otherwise, not.
        return backupContext == null || isComplete();
    }

    public boolean canStartRestore() {
        // If restoreContext is null, then we can start restore; otherwise, not.
        return backupContext == null;
    }

    public boolean inProgress() {

        return (backupContext != null && !isComplete());
    }

    public boolean isComplete() {

        return (backupContext != null &&
                backup != null && backup.isComplete() &&
                upload != null && upload.isComplete());
    }

    public List<Phase> getPhases() {
        if (backupContext == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(backup, upload);
        }
    }

}