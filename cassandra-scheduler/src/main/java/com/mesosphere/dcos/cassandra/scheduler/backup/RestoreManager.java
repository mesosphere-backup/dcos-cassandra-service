package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.LogOperationRecorder;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentReference;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.scheduler.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RestoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreManager.class);

    public static final String RESTORE_KEY = "restore";

    private EventBus eventBus;
    private RestorePlan plan;
    private PlanManager planManager;
    private PlanScheduler planScheduler;
    private OfferAccepter offerAccepter;
    private CassandraTasks cassandraTasks;
    private volatile RestoreContext context;
    private ConfigurationManager configurationManager;
    private ClusterTaskOfferRequirementProvider provider;
    private PersistentReference<RestoreContext> persistentContext;

    @Inject
    public RestoreManager(ConfigurationManager configurationManager,
                          CassandraTasks cassandraTasks,
                          EventBus eventBus,
                          ClusterTaskOfferRequirementProvider provider,
                          PersistenceFactory persistenceFactory,
                          final Serializer<RestoreContext> serializer) {
        this.eventBus = eventBus;
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;
        this.configurationManager = configurationManager;

        // Load RestoreManager from state store
        this.persistentContext = persistenceFactory.createReference(RESTORE_KEY, serializer);
        try {
            final Optional<RestoreContext> loadedContext = persistentContext.load();
            if (loadedContext.isPresent()) {
                this.context = loadedContext.get();
            }
            // Recovering from failure
            if (context != null) {
                startRestore(context);
            }
        } catch (PersistenceException e) {
            LOGGER.error("Error loading restore context from persistence store. Reason: ", e);
            throw new RuntimeException(e);
        }
    }

    public List<Protos.OfferID> resourceOffers(SchedulerDriver driver,
                                               List<Protos.Offer> offers) {
        LOGGER.info("RestoreManager got offers: {}", offers.size());

        // Check if a restore is in progress or not.
        if (this.context == null) {
            LOGGER.info("RestoreContext is null, hence no restore is in progress, ignoring offers.");
            // No restore in progress
            return Lists.newArrayList();
        }

        if (this.planManager != null) {
            if (this.planManager.planIsComplete()) {
                this.stopRestore();
            }
        }

        List<Protos.OfferID> acceptedOffers = new ArrayList<>();
        final Block currentBlock = planManager.getCurrentBlock();
        LOGGER.info("RestoreManager found next block to be scheduled: {}", currentBlock);
        acceptedOffers.addAll(
                planScheduler.resourceOffers(driver, offers, currentBlock));

        LOGGER.info("RestoreManager accepted following offers: {}", acceptedOffers);

        return acceptedOffers;
    }

    public void startRestore(RestoreContext context) {
        LOGGER.info("Starting restore with context: {}", context);

        this.offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new PersistentOperationRecorder(cassandraTasks)));
        final int servers = configurationManager.getServers();
        this.plan = new RestorePlan(context, servers, cassandraTasks, eventBus, provider);

        // TODO: Make install strategy pluggable
        this.planManager = new DefaultPlanManager(plan);
        this.planScheduler = new DefaultPlanScheduler(offerAccepter);

        try {
            persistentContext.store(context);
            this.context = context;
        } catch (PersistenceException e) {
            LOGGER.error("Error storing restore context into peristence store. Reason: ", e);
            throw new RuntimeException(e);
        }
    }

    public void stopRestore() {
        LOGGER.info("Stopping restore");
        try {
            this.persistentContext.delete();
        } catch (PersistenceException e) {
            LOGGER.error("Error deleting restore context from persistence store. Reason: {}", e);
        }
        this.context = null;
    }

    public boolean canStartRestore() {
        // If restoreContext is null, then we can start restore; otherwise, not.
        return context == null;
    }

    public RestorePlan getRestorePlan() {
        return this.plan;
    }
}
