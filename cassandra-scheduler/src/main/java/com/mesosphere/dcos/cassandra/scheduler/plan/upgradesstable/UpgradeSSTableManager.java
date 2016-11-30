package com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable;


import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.scheduler.resources.UpgradeSSTableRequest;
import org.apache.mesos.scheduler.ChainedObserver;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class UpgradeSSTableManager extends ChainedObserver implements ClusterTaskManager<UpgradeSSTableRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpgradeSSTableManager.class);
    static final String UPGRADESSTABLE_KEY = "upgradesstable";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;
    private volatile UpgradeSSTablePhase phase = null;
    private volatile UpgradeSSTableContext activeContext = null;
    private StateStore stateStore;
    private boolean enableUpgradeSSTableEndpoint = false;

    @Inject
    public UpgradeSSTableManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore,
            @Named("ConfiguredEnableUpgradeSSTableEndpoint") boolean enableUpgradeSSTableEndpoint) {
        this.provider = provider;
        this.cassandraState = cassandraState;
        this.stateStore = stateStore;
        this.enableUpgradeSSTableEndpoint = enableUpgradeSSTableEndpoint;

        // Load UpgradeSSTableManager from state store
        try {
            UpgradeSSTableContext upgradesstable = UpgradeSSTableContext.JSON_SERIALIZER.deserialize(stateStore.fetchProperty(UPGRADESSTABLE_KEY));
            // Recovering from failure
            if (upgradesstable != null) {
                this.phase = new UpgradeSSTablePhase(upgradesstable, cassandraState, provider);
                this.phase.subscribe(this);
                this.activeContext = upgradesstable;
            }
        } catch (SerializationException e) {
            LOGGER.error("Error loading upgradesstable context from persistence store. Reason: ", e);
        } catch (StateStoreException e) {
            LOGGER.warn("No upgradesstable context found.");
        }
    }

    public void start(UpgradeSSTableRequest request) {
        if (!ClusterTaskManager.canStart(this)) {
            LOGGER.warn("UpgradeSSTable already in progress: context = {}", this.activeContext);
            return;
        }

        UpgradeSSTableContext context = request.toContext(cassandraState);
        LOGGER.info("Starting upgradesstable");
        try {
            if (isComplete()) {
                for(String name: cassandraState.getUpgradeSSTableTasks().keySet()) {
                    cassandraState.remove(name);
                }
            }
            stateStore.storeProperty(UPGRADESSTABLE_KEY, UpgradeSSTableContext.JSON_SERIALIZER.serialize(context));
            this.phase = new UpgradeSSTablePhase(context, cassandraState, provider);
            this.activeContext = context;
        } catch (SerializationException | PersistenceException e) {
            LOGGER.error(
                    "Error storing upgradesstable context into persistence store" +
                            ". Reason: ",
                    e);
        }

        notifyObservers();
    }

    public void stop() {
        LOGGER.info("Stopping upgradesstable");
        try {
            stateStore.clearProperty(UPGRADESSTABLE_KEY);
            cassandraState.remove(cassandraState.getUpgradeSSTableTasks().keySet());
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting upgradesstable context from persistence store. Reason: {}",
                    e);
        }
        this.activeContext = null;

        notifyObservers();
    }

    public boolean isInProgress() {
        return activeContext != null && !isComplete();
    }

    public boolean isComplete() {
        return activeContext != null && phase != null && phase.isComplete();
    }

    public List<Phase> getPhases() {
        return phase == null ? Collections.emptyList() : Arrays.asList(phase);
    }

    public boolean isUpgradeSSTableEndpointEnabled() { return enableUpgradeSSTableEndpoint; }
}