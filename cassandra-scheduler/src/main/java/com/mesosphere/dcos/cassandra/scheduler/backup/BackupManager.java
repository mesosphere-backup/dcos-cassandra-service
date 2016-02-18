package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.LogOperationRecorder;
import io.dropwizard.setup.Environment;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.scheduler.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * BackupManager is responsible for orchestrating cluster-wide backup.
 * It also ensures that only one backup can run an anytime. For each new backup
 * a new BackupPlan is created, which will assist in orchestration.
 */
public class BackupManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupManager.class);

    private BackupPlan backupPlan;
    private Environment environment;
    private PlanManager planManager;
    private PlanScheduler planScheduler;
    private OfferAccepter offerAccepter;
    private BackupStateStore backupStateStore;
    private volatile BackupContext backupContext;
    private ConfigurationManager configurationManager;

    @Inject
    public BackupManager(Environment environment, ConfigurationManager configurationManager,
                         BackupStateStore backupStateStore, EventBus eventBus) {
        this.environment = environment;
        this.backupStateStore = backupStateStore;
        this.configurationManager = configurationManager;

        // Register BackupStateStore so that if can receive TaskStatus updates
        eventBus.register(backupStateStore);
    }

    public List<Protos.OfferID> resourceOffers(SchedulerDriver driver,
                                               List<Protos.Offer> offers) {
        // Check if a backup is in progress or not.
        if (backupContext == null) {
            // No backup in progress
            return Lists.newArrayList();
        }

        List<Protos.OfferID> acceptedOffers = new ArrayList<>();
        final Block currentBlock = planManager.getCurrentBlock();
        acceptedOffers.addAll(
                planScheduler.resourceOffers(driver, offers, currentBlock));

        // TODO: Figure out when and how to mark backup as complete. BackupStateStore statusUpdate?

        return acceptedOffers;
    }

    public void startBackup(BackupContext context) {
        this.backupContext = context;

        // TODO: Check pre-conditions
        this.offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new BackupOperationRecorder(backupStateStore)));
        this.backupPlan = new BackupPlan(context);
        // TODO: Make install strategy pluggable
        this.planManager = new DefaultPlanManager(new DefaultInstallStrategy(backupPlan));
        this.planScheduler = new DefaultPlanScheduler(offerAccepter);
    }
}