package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.LogOperationRecorder;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentReference;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPhaseStrategies;
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
    public static final String BACKUP_KEY = "backup";

    private EventBus eventBus;
    private BackupPlan backupPlan;
    private PlanManager planManager;
    private PlanScheduler planScheduler;
    private OfferAccepter offerAccepter;
    private CassandraTasks cassandraTasks;
    private volatile BackupContext backupContext;
    private ConfigurationManager configurationManager;
    private ClusterTaskOfferRequirementProvider provider;
    private PersistentReference<BackupContext> persistentBackupContext;
    private final CassandraPhaseStrategies phaseStrategies;

    @Inject
    public BackupManager(ConfigurationManager configurationManager,
                         CassandraTasks cassandraTasks,
                         EventBus eventBus,
                         ClusterTaskOfferRequirementProvider provider,
                         PersistenceFactory persistenceFactory,
                         CassandraPhaseStrategies phaseStrategies,
                         final Serializer<BackupContext> serializer) {
        this.eventBus = eventBus;
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;
        this.configurationManager = configurationManager;
        this.phaseStrategies = phaseStrategies;

        // Load BackupManager from state store
        this.persistentBackupContext = persistenceFactory.createReference(BACKUP_KEY, serializer);
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
            LOGGER.error("Error loading backup context from peristence store. Reason: ", e);
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

        // Nothing to schedule
        if (currentBlock == null) {
            LOGGER.info("Nothing to schedule as current block is null: {}", currentBlock);
            return acceptedOffers;
        }

        final int id = currentBlock.getId();
        Optional<CassandraTask> task = cassandraTasks.findCassandraDaemonTaskbyId(id);
        if (!task.isPresent()) {
            return acceptedOffers;
        }

        LOGGER.info("BackupManager found next block to be scheduled: {}", currentBlock);

        // Find the offer from slave on which we the cassandra daemon is running for this block.
        final String slaveId = task.get().getSlaveId();
        List<Protos.Offer> chosenOne = new ArrayList<>(1);
        for (Protos.Offer offer : offers) {
            if (offer.getSlaveId().getValue().equals(slaveId)) {
                LOGGER.info("Found slave on which the cassandra daemon is running: {}", slaveId);
                chosenOne.add(offer);
                break;
            }
        }

        acceptedOffers.addAll(
                planScheduler.resourceOffers(driver, chosenOne, currentBlock));

        LOGGER.info("BackupManager accepted following offers: {}", acceptedOffers);

        return acceptedOffers;
    }

    public void startBackup(BackupContext context) {
        LOGGER.info("Starting backup");

        this.offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new PersistentOperationRecorder(cassandraTasks)));
        final int servers = configurationManager.getServers();
        this.backupPlan = new BackupPlan(context, servers, cassandraTasks, eventBus, provider);

        // TODO: Make install strategy pluggable
        this.planManager = new DefaultPlanManager(backupPlan,this.phaseStrategies);
        this.planScheduler = new DefaultPlanScheduler(offerAccepter);

        try {
            persistentBackupContext.store(context);
            this.backupContext = context;
        } catch (PersistenceException e) {
            LOGGER.error("Error storing backup context into persistence store. Reason: ", e);
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