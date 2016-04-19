package com.mesosphere.dcos.cassandra.scheduler.plan.repair;


import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
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

public class RepairManager {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    RepairManager.class);
    public static final String REPAIR_KEY = "repair";

    private final CassandraTasks cassandraTasks;
    private final ClusterTaskOfferRequirementProvider provider;
    private final PersistentReference<RepairContext> persistent;
    private volatile RepairPhase phase = null;
    private volatile RepairContext context = null;

    @Inject
    public RepairManager(
            CassandraTasks cassandraTasks,
            ClusterTaskOfferRequirementProvider provider,
            PersistenceFactory persistenceFactory,
            final Serializer<RepairContext> serializer) {
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;

        // Load RepairManager from state store
        this.persistent = persistenceFactory.createReference(
                REPAIR_KEY, serializer);
        try {
            final Optional<RepairContext> loaded = persistent.load();
            if (loaded.isPresent()) {
                RepairContext repair = loaded.get();
                // Recovering from failure
                if (repair != null) {
                    this.phase = new RepairPhase(repair, cassandraTasks,
                            provider);
                    this.context = repair;
                }
            }

        } catch (PersistenceException e) {
            LOGGER.error("Error loading repair context from peristence store. " +
                    "Reason: ", e);
            throw new RuntimeException(e);
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
                persistent.store(context);
                this.phase = new RepairPhase(context, cassandraTasks,
                        provider);
                this.context = context;
            } catch (PersistenceException e) {
                LOGGER.error("Error storing repair context into persistence store. " +
                        "Reason: ", e);

            }
        }
    }

    public void stopRepair() {
        LOGGER.info("Stopping repair");
        try {
            this.persistent.delete();
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