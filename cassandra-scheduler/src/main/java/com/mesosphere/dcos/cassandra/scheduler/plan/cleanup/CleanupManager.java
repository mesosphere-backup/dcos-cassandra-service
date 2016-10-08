package com.mesosphere.dcos.cassandra.scheduler.plan.cleanup;


import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.resources.CleanupRequest;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;
import org.apache.mesos.scheduler.ChainedObserver;
import org.apache.mesos.scheduler.DefaultObservable;
import org.apache.mesos.scheduler.Observable;
import org.apache.mesos.scheduler.Observer;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CleanupManager extends ChainedObserver implements ClusterTaskManager<CleanupRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanupManager.class);
    static final String CLEANUP_KEY = "cleanup";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;
    private volatile CleanupPhase phase = null;
    private volatile CleanupContext activeContext = null;
    private StateStore stateStore;

    @Inject
    public CleanupManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        this.provider = provider;
        this.cassandraState = cassandraState;
        this.stateStore = stateStore;

        // Load CleanupManager from state store
        try {
            CleanupContext cleanup = CleanupContext.JSON_SERIALIZER.deserialize(stateStore.fetchProperty(CLEANUP_KEY));
            // Recovering from failure
            if (cleanup != null) {
                this.phase = new CleanupPhase(cleanup, cassandraState, provider);
                this.phase.subscribe(this);
                this.activeContext = cleanup;
            }
        } catch (SerializationException e) {
            LOGGER.error("Error loading cleanup context from persistence store. Reason: ", e);
        } catch (StateStoreException e) {
            LOGGER.warn("No backup context found.");
        }
    }


    public void start(CleanupRequest request) {
        if (!ClusterTaskManager.canStart(this)) {
            LOGGER.warn("Cleanup already in progress: context = {}", this.activeContext);
            return;
        }

        CleanupContext context = request.toContext(cassandraState);
        LOGGER.info("Starting cleanup");
        try {
            if (isComplete()) {
                for(String name: cassandraState.getCleanupTasks().keySet()) {
                    cassandraState.remove(name);
                }
            }
            stateStore.storeProperty(CLEANUP_KEY, CleanupContext.JSON_SERIALIZER.serialize(context));
            this.phase = new CleanupPhase(context, cassandraState, provider);
            this.activeContext = context;
        } catch (SerializationException | PersistenceException e) {
            LOGGER.error(
                    "Error storing cleanup context into persistence store" +
                            ". Reason: ",
                    e);
        }

        notifyObservers();
    }

    public void stop() {
        LOGGER.info("Stopping cleanup");
        try {
            stateStore.clearProperty(CLEANUP_KEY);
            cassandraState.remove(cassandraState.getCleanupTasks().keySet());
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting cleanup context from persistence store. Reason: {}",
                    e);
        }
        this.activeContext = null;

        notifyObservers();
    }

    public boolean isInProgress() {
        return (activeContext != null && !isComplete());
    }

    public boolean isComplete() {
        return (activeContext != null &&
                phase != null && phase.isComplete());
    }

    public List<Phase> getPhases() {
        if (phase == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(phase);
        }
    }
}