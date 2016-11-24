package com.mesosphere.dcos.cassandra.scheduler;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.offer.LogOperationRecorder;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPlan;
import com.mesosphere.dcos.cassandra.scheduler.plan.DeploymentManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable.UpgradeSSTableManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.offer.OfferEvaluator;
import org.apache.mesos.offer.ResourceCleaner;
import org.apache.mesos.offer.ResourceCleanerScheduler;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.DefaultTaskKiller;
import org.apache.mesos.scheduler.Observable;
import org.apache.mesos.scheduler.Observer;
import org.apache.mesos.scheduler.SchedulerDriverFactory;
import org.apache.mesos.scheduler.TaskKiller;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.recovery.DefaultTaskFailureListener;
import org.apache.mesos.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class CassandraScheduler implements Scheduler, Managed, Observer {
    private final static Logger LOGGER = LoggerFactory.getLogger(
            CassandraScheduler.class);

    private SchedulerDriver driver;
    private final ConfigurationManager configurationManager;
    private final MesosConfig mesosConfig;
    private PlanManager planManager;
    private CassandraRecoveryScheduler recoveryScheduler;
    private final OfferAccepter offerAccepter;
    private final PersistentOfferRequirementProvider offerRequirementProvider;
    private final CassandraState cassandraState;
    private final Reconciler reconciler;
    private final SchedulerClient client;
    private final ScheduledExecutorService executor;
    private final StateStore stateStore;
    private final DefaultConfigurationManager defaultConfigurationManager;
    private final BackupManager backupManager;
    private final RestoreManager restoreManager;
    private final CleanupManager cleanupManager;
    private final RepairManager repairManager;
    private final UpgradeSSTableManager upgradeSSTableManager;
    private final Protos.Filters offerFilters;
    private PlanScheduler planScheduler;

    private static TaskKiller taskKiller;

    @Inject
    public CassandraScheduler(
            final ConfigurationManager configurationManager,
            final MesosConfig mesosConfig,
            final PersistentOfferRequirementProvider offerRequirementProvider,
            final CassandraState cassandraState,
            final SchedulerClient client,
            final ScheduledExecutorService executor,
            final StateStore stateStore,
            final DefaultConfigurationManager defaultConfigurationManager,
            final BackupManager backupManager,
            final RestoreManager restoreManager,
            final CleanupManager cleanupManager,
            final RepairManager repairManager,
            final UpgradeSSTableManager upgradeSSTableManager) {
        this.mesosConfig = mesosConfig;
        this.cassandraState = cassandraState;
        this.reconciler = new DefaultReconciler(cassandraState.getStateStore());
        this.configurationManager = configurationManager;
        this.offerRequirementProvider = offerRequirementProvider;
        offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new PersistentOperationRecorder(cassandraState)));
        recoveryScheduler = new CassandraRecoveryScheduler(
                offerRequirementProvider,
                offerAccepter,
                cassandraState);
        recoveryScheduler.subscribe(this);
        this.client = client;
        this.executor = executor;
        this.stateStore = stateStore;
        this.defaultConfigurationManager = defaultConfigurationManager;
        this.backupManager = backupManager;
        this.restoreManager = restoreManager;
        this.cleanupManager = cleanupManager;
        this.repairManager = repairManager;
        this.upgradeSSTableManager = upgradeSSTableManager;

        this.offerFilters = Protos.Filters.newBuilder().setRefuseSeconds(mesosConfig.getRefuseSeconds()).build();
        LOGGER.info("Creating an offer filter with refuse_seconds = {}", mesosConfig.getRefuseSeconds());
    }

    @Override
    public void start() throws Exception {
        registerFramework();
    }

    @Override
    public void stop() throws Exception {
        if (this.driver != null) {
            LOGGER.info("Aborting driver...");
            final Protos.Status driverStatus = this.driver.abort();
            LOGGER.info("Aborted driver with status: {}", driverStatus);
        }
    }

    @Override
    public void registered(SchedulerDriver driver,
                           Protos.FrameworkID frameworkId,
                           Protos.MasterInfo masterInfo) {
        final String frameworkIdValue = frameworkId.getValue();
        LOGGER.info("Framework registered : id = {}", frameworkIdValue);
        try {
            CassandraScheduler.taskKiller = new DefaultTaskKiller(
                    stateStore,
                    new DefaultTaskFailureListener(stateStore),
                    driver);
            this.planScheduler = new DefaultPlanScheduler(
                    offerAccepter,
                    new OfferEvaluator(cassandraState.getStateStore()),
                    taskKiller);
            stateStore.storeFrameworkId(frameworkId);
            Plan plan = CassandraPlan.create(
                    defaultConfigurationManager,
                    DeploymentManager.create(
                            offerRequirementProvider,
                            configurationManager,
                            defaultConfigurationManager,
                            cassandraState,
                            client,
                            reconciler,
                            new SeedsManager(defaultConfigurationManager, cassandraState, executor, client, stateStore),
                            executor
                    ),
                    backupManager,
                    restoreManager,
                    cleanupManager,
                    repairManager,
                    upgradeSSTableManager);
            plan.subscribe(this);
            planManager = new DefaultPlanManager(plan);
            reconciler.start();
            suppressOrRevive();
        } catch (Throwable t) {
            String error = "An error occurred when registering " +
                    "the framework and initializing the execution plan.";
            LOGGER.error(error, t);
            throw new RuntimeException(error, t);
        }
    }

    @Override
    public void reregistered(SchedulerDriver driver,
                             Protos.MasterInfo masterInfo) {
        LOGGER.info("Re-registered with master: {}", masterInfo);
        reconciler.start();
        suppressOrRevive();
    }

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Protos.Offer> offers) {
        logOffers(offers);
        reconciler.reconcile(driver);

        try {
            final List<Protos.OfferID> acceptedOffers = new ArrayList<>();

            final Collection<? extends Step> currentSteps = planManager.getCandidates(Collections.emptyList());

            LOGGER.info("Current execution steps = {}", currentSteps);
            if (!currentSteps.isEmpty()) {
                try {
                    acceptedOffers.addAll(planScheduler.resourceOffers(driver, offers, currentSteps));
                } catch (Throwable t) {
                    LOGGER.error("Error occurred with plan scheduler:", t);
                }
            }
            // Perform any required repairs
            final List<Protos.Offer> unacceptedOffers = filterAcceptedOffers(offers, acceptedOffers);
            try {
                acceptedOffers.addAll(recoveryScheduler.resourceOffers(
                        driver,
                        unacceptedOffers,
                        currentSteps.stream().map(step -> step.getName()).collect(Collectors.toSet())));
            } catch (Throwable t) {
                LOGGER.error("Error occured with plan scheduler:", t);
            }

            ResourceCleanerScheduler cleanerScheduler = getCleanerScheduler();
            if (cleanerScheduler != null) {
                try {
                    acceptedOffers.addAll(getCleanerScheduler().resourceOffers(driver, offers));
                } catch (Throwable t) {
                    LOGGER.error("Error occured with cleaner scheduler:", t);
                }
            }

            declineOffers(driver, acceptedOffers, offers);
        } catch (Throwable t){
            LOGGER.error("Error in offer acceptance cycle", t);
        }
    }

    private ResourceCleanerScheduler getCleanerScheduler() {
        try {
            ResourceCleaner cleaner = new ResourceCleaner(cassandraState.getStateStore());
            return new ResourceCleanerScheduler(cleaner, offerAccepter);
        } catch (Exception ex) {
            LOGGER.error("Failed to construct ResourceCleaner with exception:", ex);
            return null;
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOGGER.info("Offer rescinded. offerId: {}", offerId.getValue());
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info(
                "Received status update for taskId={} state={} source={} reason={} message='{}'",
                status.getTaskId().getValue(),
                status.getState().toString(),
                status.getSource().name(),
                status.getReason().name(),
                status.getMessage());
        try {
            cassandraState.update(status);
        } catch (Exception ex) {
            LOGGER.error("Error updating Tasks with status: {} reason: {}", status, ex);
        }
        try {
            planManager.update(status);
        } catch (Exception ex) {
            LOGGER.error("Error updating Stage Manager with status: {} reason: {}", status, ex);
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 Protos.ExecutorID executorId,
                                 Protos.SlaveID slaveId,
                                 byte[] data) {
        LOGGER.info("Framework message: executorId={} slaveId={} data='{}'",
                executorId.getValue(), slaveId.getValue(),
                Arrays.toString(data));
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOGGER.info("Scheduler driver disconnected.");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        LOGGER.info("Slave lost slaveId: {}", slaveId.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver driver,
                             Protos.ExecutorID executorId,
                             Protos.SlaveID slaveId,
                             int status) {
        LOGGER.info("Executor lost: executorId: {} slaveId: {} status: {}",
                executorId.getValue(), slaveId.getValue(), status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("Scheduler driver error: {}", message);
    }

    private List<Protos.Offer> filterAcceptedOffers(List<Protos.Offer> offers,
                                                    List<Protos.OfferID> acceptedOfferIds) {
        return offers.stream().filter(
                offer -> !offerAccepted(offer, acceptedOfferIds)).collect(
                Collectors.toList());
    }

    private boolean offerAccepted(Protos.Offer offer,
                                  List<Protos.OfferID> acceptedOfferIds) {
        return acceptedOfferIds.stream().anyMatch(
                acceptedOfferId -> acceptedOfferId.equals(offer.getId()));
    }

    private void registerFramework() throws IOException {
        final SchedulerDriverFactory factory = new SchedulerDriverFactory();
        final CassandraSchedulerConfiguration targetConfig =
                (CassandraSchedulerConfiguration) defaultConfigurationManager.getTargetConfig();
        final ServiceConfig serviceConfig = targetConfig.getServiceConfig();
        final Optional<ByteString> secretBytes = serviceConfig.readSecretBytes();
        final Protos.FrameworkInfo.Builder builder = Protos.FrameworkInfo.newBuilder()
                .setRole(serviceConfig.getRole())
                .setUser(serviceConfig.getUser())
                .setName(targetConfig.getServiceConfig().getName())
                .setPrincipal(serviceConfig.getPrincipal())
                .setCheckpoint(serviceConfig.isCheckpoint())
                .setFailoverTimeout(serviceConfig.getFailoverTimeoutS());

        Optional<Protos.FrameworkID> frameworkID = stateStore.fetchFrameworkId();
        if (frameworkID.isPresent() && frameworkID.get().hasValue()) {
            builder.setId(frameworkID.get());
        } else {
            LOGGER.info("No framework id found");
        }

        final Protos.FrameworkInfo frameworkInfo = builder.build();

        if (secretBytes.isPresent()) {
            // Authenticated if a non empty secret is provided.
            setSchedulerDriver(factory.create(
                    this,
                    frameworkInfo,
                    mesosConfig.toZooKeeperUrl(),
                    secretBytes.get().toByteArray()));
        } else {
            setSchedulerDriver(factory.create(
                    this,
                    frameworkInfo,
                    mesosConfig.toZooKeeperUrl()));
        }
        LOGGER.info("Starting driver...");
        final Protos.Status startStatus = this.driver.start();
        LOGGER.info("Driver started with status: {}", startStatus);
    }

    public void setSchedulerDriver(SchedulerDriver driver) {
        this.driver = driver;
    }

    private void logOffers(List<Protos.Offer> offers) {
        if (Objects.isNull(offers)) {
            return;
        }

        LOGGER.info("Received {} offers", offers.size());

        for (Protos.Offer offer : offers) {
            LOGGER.info("Received Offer: {}", TextFormat.shortDebugString(offer));
        }
    }

    private void declineOffers(SchedulerDriver driver,
                               List<Protos.OfferID> acceptedOffers,
                               List<Protos.Offer> offers) {
        for (Protos.Offer offer : offers) {
            if (!acceptedOffers.contains(offer.getId())) {
                declineOffer(driver, offer);
            }
        }
    }

    private void declineOffer(SchedulerDriver driver, Protos.Offer offer) {
        Protos.OfferID offerId = offer.getId();
        LOGGER.info("Scheduler declining offer: {}", offerId);
        driver.declineOffer(offerId, offerFilters);
    }

    private void reviveOffers() {
        LOGGER.info("Reviving offers.");
        driver.reviveOffers();
        cassandraState.setSuppressed(false);
    }

    private void suppressOffers() {
        LOGGER.info("Suppressing offers.");
        driver.suppressOffers();
        cassandraState.setSuppressed(true);
    }

    private boolean hasOperations() {
        boolean hasOperations = !planManager.getPlan().isComplete() ||
                recoveryScheduler.hasOperations();

        LOGGER.debug(hasOperations ?
                "Scheduler has operations to perform." :
                "Scheduler has no operations to perform.");
        return hasOperations;
    }

    @Override
    public void update(Observable observable) {
        if (observable == planManager.getPlan() ||
            observable == recoveryScheduler) {
            suppressOrRevive();
        }
    }

    private void suppressOrRevive() {
        if (hasOperations()) {
            reviveOffers();
        } else {
            suppressOffers();
        }
    }

    public static TaskKiller getTaskKiller() {
        return taskKiller;
    }
}
