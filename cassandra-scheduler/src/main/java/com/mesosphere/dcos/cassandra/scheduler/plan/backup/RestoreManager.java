package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
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

public class RestoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreManager.class);

    public static final String RESTORE_KEY = "restore";

    private CassandraTasks cassandraTasks;
    private final ClusterTaskOfferRequirementProvider provider;
    private final PersistentReference<RestoreContext> persistentContext;
    private volatile RestoreContext context = null;
    private volatile DownloadSnapshotPhase download = null;
    private volatile RestoreSnapshotPhase restore = null;

    @Inject
    public RestoreManager(
            final CassandraTasks cassandraTasks,
            final ClusterTaskOfferRequirementProvider provider,
            final PersistenceFactory persistenceFactory,
            final Serializer<RestoreContext> serializer) {
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;

        // Load RestoreManager from state store
        this.persistentContext = persistenceFactory.createReference(RESTORE_KEY,
                serializer);
        try {
            final Optional<RestoreContext> loadedContext = persistentContext.load();
            if (loadedContext.isPresent()) {
                RestoreContext context = loadedContext.get();
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
            }
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error loading restore context from persistence store. Reason: ",
                    e);
            throw new RuntimeException(e);
        }
    }

    public void startRestore(RestoreContext context) {

        if (canStartRestore()) {
            LOGGER.info("Starting restore");
            try {
                if(isComplete()){
                    for(String name:
                            cassandraTasks.getDownloadSnapshotTasks().keySet()){
                        cassandraTasks.remove(name);
                    }
                    for(String name:
                            cassandraTasks.getRestoreSnapshotTasks().keySet()){
                        cassandraTasks.remove(name);
                    }
                }
                persistentContext.store(context);
                this.download = new DownloadSnapshotPhase(
                        context,
                        cassandraTasks,
                        provider);
                this.restore = new RestoreSnapshotPhase(
                        context,
                        cassandraTasks,
                        provider);
                //this volatile singles that restore is started
                this.context = context;

            } catch (PersistenceException e) {
                LOGGER.error(
                        "Error storing restore context into persistence store. Reason: ",
                        e);
                this.context = null;
            }
        } else {

            LOGGER.warn("Restore already in progress: context = ", this.context);
        }
    }

    public void stopRestore() {
        LOGGER.info("Stopping restore");
        try {
            this.persistentContext.delete();
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting restore context from persistence store. Reason: {}",
                    e);
        }
        this.context = null;
        this.download = null;
        this.restore = null;
    }

    public boolean canStartRestore() {
        // If restoreContext is null, then we can start restore; otherwise, not.
        return context == null || isComplete();
    }

    public boolean inProgress() {

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
