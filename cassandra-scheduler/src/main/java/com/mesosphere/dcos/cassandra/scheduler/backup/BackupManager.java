package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
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

/**
 * BackupManager is responsible for orchestrating cluster-wide backup.
 * It also ensures that only one backup can run an anytime. For each new backup
 * a new BackupPlan is created, which will assist in orchestration.
 */
public class BackupManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupManager.class);

    private EventBus eventBus;
    private BackupPlan backupPlan;
    private PlanManager planManager;
    private PlanScheduler planScheduler;
    private OfferAccepter offerAccepter;
    private CassandraTasks cassandraTasks;
    private volatile BackupContext backupContext;
    private PersistenceFactory persistenceFactory;
    private ConfigurationManager configurationManager;
    private ClusterTaskOfferRequirementProvider provider;
    private PersistentReference<BackupContext> persistentBackupContext;

    @Inject
    public BackupManager(ConfigurationManager configurationManager,
                         CassandraTasks cassandraTasks,
                         EventBus eventBus,
                         ClusterTaskOfferRequirementProvider provider,
                         PersistenceFactory persistenceFactory,
                         final Serializer<BackupContext> serializer) {
        this.eventBus = eventBus;
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;
        this.persistenceFactory = persistenceFactory;
        this.configurationManager = configurationManager;

        // Load BackupManager from state store
        this.persistentBackupContext = persistenceFactory.createReference("backup", serializer);
        try {
            final Optional<BackupContext> loadedBackupContext = persistentBackupContext.load();
            if (loadedBackupContext.isPresent()) {
                this.backupContext = loadedBackupContext.get();
            }
            // Recovering from failure
            if (backupContext != null) {
                startBackup(backupContext);
            }
        } catch (PersistenceException e) {
            LOGGER.error("Error loading backup contenxt from peristence store. Reason: ", e);
            throw new RuntimeException(e);
        }
    }

    public List<Protos.OfferID> resourceOffers(SchedulerDriver driver,
                                               List<Protos.Offer> offers) {
        LOGGER.info("BackupManager got offers: {}", offers.size());

        // Check if a backup is in progress or not.
        if (this.backupContext == null) {
            LOGGER.info("BackupContext is null, hence no backup is in progress, ignoring offers.");
            // No backup in progress
            return Lists.newArrayList();
        }

        if (this.planManager != null) {
            if (this.planManager.planIsComplete()) {
                this.stopBackup();
            }
        }

        List<Protos.OfferID> acceptedOffers = new ArrayList<>();
        final Block currentBlock = planManager.getCurrentBlock();
        LOGGER.info("BackupManager found next block to be scheduled: {}", currentBlock);
        acceptedOffers.addAll(
                planScheduler.resourceOffers(driver, offers, currentBlock));

        LOGGER.info("BackupManager accepted following offers: {}", acceptedOffers);

        return acceptedOffers;
    }

    public void startBackup(BackupContext context) {
        LOGGER.info("Starting backup with context: {}", context);

        this.offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new PersistentOperationRecorder(cassandraTasks)));
        final int servers = configurationManager.getServers();
        this.backupPlan = new BackupPlan(context, servers, cassandraTasks, eventBus, provider);

        // TODO: Make install strategy pluggable
        this.planManager = new DefaultPlanManager(backupPlan);
        this.planScheduler = new DefaultPlanScheduler(offerAccepter);

        try {
            persistentBackupContext.store(context);
            this.backupContext = context;
        } catch (PersistenceException e) {
            LOGGER.error("Error storing backup contenxt from peristence store. Reason: ", e);
            throw new RuntimeException(e);
        }
    }

    public void stopBackup() {
        LOGGER.info("Stopping backup");
        try {
            this.persistentBackupContext.delete();
        } catch (PersistenceException e) {
            LOGGER.error("Error deleting backup context from persistence store. Reason: {}", e);
        }
        this.backupContext = null;
    }

    public boolean canStartBackup() {
        // If backupContext is null, then we can start backup; otherwise, not.
        return backupContext == null;
    }

    public BackupPlan getBackupPlan() {
        return this.backupPlan;
    }
}