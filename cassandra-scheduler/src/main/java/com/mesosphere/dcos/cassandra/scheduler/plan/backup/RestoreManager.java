package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RestoreManager implements ClusterTaskManager<BackupRestoreContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreManager.class);
    static final String RESTORE_KEY = "restore";

    private CassandraTasks cassandraTasks;
    private final ClusterTaskOfferRequirementProvider provider;
    private volatile BackupRestoreContext context = null;
    private volatile DownloadSnapshotPhase download = null;
    private volatile RestoreSnapshotPhase restore = null;
    private StateStore stateStore;

    @Inject
    public RestoreManager(
            final CassandraTasks cassandraTasks,
            final ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;
        this.stateStore = stateStore;
        // Load RestoreManager from state store
        try {
            BackupRestoreContext context = BackupRestoreContext.JSON_SERIALIZER.deserialize(stateStore.fetchProperty(RESTORE_KEY));
            // Recovering from failure
            if (context != null) {
                this.download = new DownloadSnapshotPhase(
                        context,
                        cassandraTasks,
                        provider);
                this.restore = new RestoreSnapshotPhase(
                        context,
                        cassandraTasks,
                        provider);
                this.context = context;
            }
        } catch (SerializationException e) {
            LOGGER.error("Error loading restore context from persistence store. Reason: ", e);
        } catch (StateStoreException e) {
            LOGGER.warn("No backup context found.");
        }
    }

    public void start(BackupRestoreContext context) {
        if (!ClusterTaskManager.canStart(this)) {
            LOGGER.warn("Restore already in progress: context = {}", this.context);
            return;
        }

        LOGGER.info("Starting restore");
        try {
            if (isComplete()) {
                for(String name:
                        cassandraTasks.getDownloadSnapshotTasks().keySet()){
                    cassandraTasks.remove(name);
                }
                for(String name:
                        cassandraTasks.getRestoreSnapshotTasks().keySet()){
                    cassandraTasks.remove(name);
                }
            }
            stateStore.storeProperty(RESTORE_KEY, BackupRestoreContext.JSON_SERIALIZER.serialize(context));
            this.download = new DownloadSnapshotPhase(
                    context,
                    cassandraTasks,
                    provider);
            this.restore = new RestoreSnapshotPhase(
                    context,
                    cassandraTasks,
                    provider);
            //this volatile signals that restore is started
            this.context = context;
        } catch (SerializationException | PersistenceException e) {
            LOGGER.error(
                    "Error storing restore context into persistence store. Reason: ",
                    e);
            this.context = null;
        }
    }

    public void stop() {
        LOGGER.info("Stopping restore");
        try {
            // TODO: Delete restore context from Property store
            stateStore.clearProperty(RESTORE_KEY);
            cassandraTasks.remove(cassandraTasks.getRestoreSnapshotTasks().keySet());
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting restore context from persistence store. Reason: {}",
                    e);
        }
        this.context = null;
        this.download = null;
        this.restore = null;
    }

    public boolean isInProgress() {
        return (context != null && !isComplete());
    }

    public boolean isComplete() {
        return (context != null &&
                download != null && download.isComplete() &&
                restore != null && restore.isComplete());
    }

    public List<Phase> getPhases() {
        if (context == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(download, restore);
        }
    }
}
