package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.resources.BackupRestoreRequest;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;

import org.apache.mesos.scheduler.ChainedObserver;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RestoreManager extends ChainedObserver implements ClusterTaskManager<BackupRestoreRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreManager.class);
    static final String RESTORE_KEY = "restore";

    private CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;
    private volatile BackupRestoreContext activeContext = null;
    private volatile DownloadSnapshotPhase download = null;
    private volatile RestoreSnapshotPhase restore = null;
    private StateStore stateStore;

    @Inject
    public RestoreManager(
            final CassandraState cassandraState,
            final ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        this.provider = provider;
        this.cassandraState = cassandraState;
        this.stateStore = stateStore;
        // Load RestoreManager from state store
        try {
            BackupRestoreContext context = BackupRestoreContext.JSON_SERIALIZER.deserialize(stateStore.fetchProperty(RESTORE_KEY));
            // Recovering from failure
            if (context != null) {
                this.download = new DownloadSnapshotPhase(
                        context,
                        cassandraState,
                        provider);
                this.restore = new RestoreSnapshotPhase(
                        context,
                        cassandraState,
                        provider);
                this.activeContext = context;
            }
        } catch (SerializationException e) {
            LOGGER.error("Error loading restore context from persistence store. Reason: ", e);
        } catch (StateStoreException e) {
            LOGGER.warn("No backup context found.");
        }
    }

    public void start(BackupRestoreRequest request) {
        if (!ClusterTaskManager.canStart(this)) {
            LOGGER.warn("Restore already in progress: context = {}", this.activeContext);
            return;
        }

        BackupRestoreContext context = request.toContext();
        LOGGER.info("Starting restore");
        try {
            if (isComplete()) {
                for(String name:
                        cassandraState.getDownloadSnapshotTasks().keySet()){
                    cassandraState.remove(name);
                }
                for(String name:
                        cassandraState.getRestoreSnapshotTasks().keySet()){
                    cassandraState.remove(name);
                }
            }
            stateStore.storeProperty(RESTORE_KEY, BackupRestoreContext.JSON_SERIALIZER.serialize(context));
            this.download = new DownloadSnapshotPhase(
                    context,
                    cassandraState,
                    provider);
            this.restore = new RestoreSnapshotPhase(
                    context,
                    cassandraState,
                    provider);
            this.download.subscribe(this);
            this.restore.subscribe(this);
            //this volatile signals that restore is started
            this.activeContext = context;
        } catch (SerializationException | PersistenceException e) {
            LOGGER.error(
                    "Error storing restore context into persistence store. Reason: ",
                    e);
            this.activeContext = null;
        }

        notifyObservers();
    }

    public void stop() {
        LOGGER.info("Stopping restore");
        try {
            // TODO: Delete restore context from Property store
            stateStore.clearProperty(RESTORE_KEY);
            cassandraState.remove(cassandraState.getRestoreSnapshotTasks().keySet());
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting restore context from persistence store. Reason: {}",
                    e);
        }
        this.activeContext = null;
        this.download = null;
        this.restore = null;

        notifyObservers();
    }

    public boolean isInProgress() {
        return (activeContext != null && !isComplete());
    }

    public boolean isComplete() {
        return (activeContext != null &&
                download != null && download.isComplete() &&
                restore != null && restore.isComplete());
    }

    public List<Phase> getPhases() {
        if (activeContext == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(download, restore);
        }
    }
}
