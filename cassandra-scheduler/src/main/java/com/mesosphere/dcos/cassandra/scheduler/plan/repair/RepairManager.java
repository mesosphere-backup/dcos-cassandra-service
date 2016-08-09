package com.mesosphere.dcos.cassandra.scheduler.plan.repair;


import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
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

public class RepairManager {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    RepairManager.class);
    public static final String REPAIR_KEY = "repair";

    private final CassandraTasks cassandraTasks;
    private final ClusterTaskOfferRequirementProvider provider;
    private volatile RepairPhase phase = null;
    private volatile RepairContext context = null;
    private StateStore stateStore;

    @Inject
    public RepairManager(
            CassandraTasks cassandraTasks,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;
        this.stateStore = stateStore;

        // Load RepairManager from state store
        try {
            RepairContext repair = RepairContext.JSON_SERIALIZER.deserialize(stateStore.fetchProperty(REPAIR_KEY));
            // Recovering from failure
            if (repair != null) {
                this.phase = new RepairPhase(repair, cassandraTasks,
                        provider);
                this.context = repair;
            }
        } catch (SerializationException e) {
            LOGGER.error("Error loading repair context from persistence store. Reason: ", e);
        } catch (StateStoreException e) {
            LOGGER.warn("No backup context found.");
        }
    }


    public void startRepair(RepairContext context) {
        LOGGER.info("Starting repair");

        if (canStartRepair()) {
            try {
                if(isComplete()){
                    for(String name: cassandraTasks.getRepairTasks().keySet()) {
                        cassandraTasks.remove(name);
                    }
                }
                stateStore.storeProperty(REPAIR_KEY, RepairContext.JSON_SERIALIZER.serialize(context));
                this.phase = new RepairPhase(context, cassandraTasks,
                        provider);
                this.context = context;
            } catch (SerializationException | PersistenceException e) {
                LOGGER.error("Error storing repair context into persistence store. " +
                        "Reason: ", e);

            }
        }
    }

    public void stopRepair() {
        LOGGER.info("Stopping repair");
        try {
            stateStore.clearProperty(REPAIR_KEY);
            cassandraTasks.remove(cassandraTasks.getRepairTasks().keySet());
        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error deleting repair context from persistence store. " +
                            "Reason: {}",
                    e);
        }
        this.context = null;
    }

    public boolean canStartRepair() {
        return context == null || isComplete();
    }


    public boolean inProgress() {

        return (context != null && !isComplete());
    }

    public boolean isComplete() {

        return (context != null &&
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