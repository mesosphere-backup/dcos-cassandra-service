package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.resources.BackupRestoreRequest;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;

import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * BackupManager is responsible for orchestrating cluster-wide backup.
 * It also ensures that only one backup can run an anytime. For each new backup
 * a new BackupPlan is created, which will assist in orchestration.
 */
public class BackupManager implements ClusterTaskManager<BackupRestoreRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupManager.class);
    static final String BACKUP_KEY = "backup";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;
    private volatile BackupSnapshotPhase backup = null;
    private volatile UploadBackupPhase upload = null;
    private StateStore stateStore;
    private volatile BackupRestoreContext activeContext = null;

    @Inject
    public BackupManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        this.provider = provider;
        this.cassandraState = cassandraState;
        this.stateStore = stateStore;

        // Load BackupManager from state store
        try {
            final byte[] bytes = stateStore.fetchProperty(BACKUP_KEY);
            final BackupRestoreContext context = BackupRestoreContext.JSON_SERIALIZER.deserialize(bytes);
            // Recovering from failure
            if (context != null) {
                this.backup = new BackupSnapshotPhase(
                        context,
                        cassandraState,
                        provider);
                this.upload = new UploadBackupPhase(
                        context,
                        cassandraState,
                        provider);
                this.activeContext = context;
            }
        } catch (SerializationException e) {
            LOGGER.error("Error loading backup context from persistence store. Reason: ", e);
        } catch (StateStoreException e) {
            LOGGER.warn("No backup context found.");
        }
    }


    public void start(BackupRestoreRequest request) {
        if (!ClusterTaskManager.canStart(this)) {
            LOGGER.warn("Backup already in progress: context = {}", this.activeContext);
            return;
        }

        BackupRestoreContext context = request.toContext();
        LOGGER.info("Starting backup");
        try {
            if (isComplete()) {
                for (String name : cassandraState.getBackupSnapshotTasks().keySet()) {
                    cassandraState.remove(name);
                }
                for (String name : cassandraState.getBackupUploadTasks().keySet()) {
                    cassandraState.remove(name);
                }
            }
            stateStore.storeProperty(BACKUP_KEY, BackupRestoreContext.JSON_SERIALIZER.serialize(context));
            this.backup = new BackupSnapshotPhase(context, cassandraState, provider);
            this.upload = new UploadBackupPhase(context, cassandraState, provider);
            //this volatile signals that backup is started
            activeContext = context;
        } catch (SerializationException | PersistenceException e) {
            LOGGER.error(
                    "Error storing backup context into persistence store. Reason: ",
                    e);
        }
    }

    public void stop() {
        LOGGER.info("Stopping backup");
        try {
            stateStore.clearProperty(BACKUP_KEY);
            cassandraState.remove(cassandraState.getBackupSnapshotTasks().keySet());
            cassandraState.remove(cassandraState.getBackupUploadTasks().keySet());
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting backup context from persistence store. Reason: {}",
                    e);
        }
        this.activeContext = null;
    }

    public boolean isInProgress() {
        return (activeContext != null && !isComplete());
    }

    public boolean isComplete() {
        return (activeContext != null &&
                backup != null && backup.isComplete() &&
                upload != null && upload.isComplete());
    }

    public List<Phase> getPhases() {
        if (activeContext == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(backup, upload);
        }
    }
}