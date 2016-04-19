package com.mesosphere.dcos.cassandra.scheduler.plan.cleanup;


import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
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

public class CleanupManager {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    CleanupManager.class);
    public static final String CLEANUP_KEY = "cleanup";

    private final CassandraTasks cassandraTasks;
    private final ClusterTaskOfferRequirementProvider provider;
    private final PersistentReference<CleanupContext> persistent;
    private volatile CleanupPhase phase = null;
    private volatile CleanupContext context = null;

    @Inject
    public CleanupManager(
            CassandraTasks cassandraTasks,
            ClusterTaskOfferRequirementProvider provider,
            PersistenceFactory persistenceFactory,
            final Serializer<CleanupContext> serializer) {
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;

        // Load CleanupManager from state store
        this.persistent = persistenceFactory.createReference(
                CLEANUP_KEY, serializer);
        try {
            final Optional<CleanupContext> loaded = persistent.load();
            if (loaded.isPresent()) {
                CleanupContext cleanup = loaded.get();
                // Recovering from failure
                if (cleanup != null) {
                    this.phase = new CleanupPhase(cleanup, cassandraTasks,
                            provider);

                    this.context = cleanup;
                }
            }

        } catch (PersistenceException e) {
            LOGGER.error(
                    "Error loading cleanup context from peristence store. " +
                            "Reason: ",
                    e);
            throw new RuntimeException(e);
        }
    }


    public void startCleanup(CleanupContext context) {
        LOGGER.info("Starting cleanup");

        if (canStartCleanup()) {
            try {
                if(isComplete()) {
                    for(String name: cassandraTasks.getCleanupTasks().keySet()) {
                        cassandraTasks.remove(name);
                    }
                }
                persistent.store(context);
                this.phase = new CleanupPhase(context, cassandraTasks,
                        provider);
                this.context = context;
            } catch (PersistenceException e) {
                LOGGER.error(
                        "Error storing cleanup context into persistence store" +
                                ". Reason: ",
                        e);

            }
        }
    }

    public boolean canStartCleanup() {
        // If CleanupContext is null, then we can start cleanup; otherwise, not.
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