package com.mesosphere.dcos.cassandra.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.offer.LogOperationRecorder;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraDaemonPhase;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPlan;
import com.mesosphere.dcos.cassandra.scheduler.plan.SyncDataCenterPhase;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable.UpgradeSSTableManager;
import com.mesosphere.dcos.cassandra.scheduler.resources.*;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.dcos.Capabilities;
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
import org.apache.mesos.scheduler.plan.api.PlansResource;
import org.apache.mesos.scheduler.recovery.DefaultTaskFailureListener;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.api.JsonPropertyDeserializer;
import org.apache.mesos.state.api.StateResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CassandraScheduler implements Scheduler, Observer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);

    private final MesosConfig mesosConfig;
    private final OfferAccepter offerAccepter;
    private final PersistentOfferRequirementProvider offerRequirementProvider;
    private final CassandraState cassandraState;
    private final Reconciler reconciler;
    private final SchedulerClient client;
    private final BackupManager backup;
    private final RestoreManager restore;
    private final CleanupManager cleanup;
    private final RepairManager repair;
    private final UpgradeSSTableManager upgrade;
    private final boolean enableUpgradeSSTableEndpoint;
    private final SeedsManager seeds;
    private final ScheduledExecutorService executor;
    private final StateStore stateStore;
    private final DefaultConfigurationManager defaultConfigurationManager;
    private final Protos.Filters offerFilters;
    private final Capabilities capabilities;
    private final ConfigurationManager configurationManager;

    private final BlockingQueue<Collection<Object>> resourcesQueue = new ArrayBlockingQueue<>(1);
    private AtomicBoolean isSchedulerRegistered = new AtomicBoolean(false);

    private static TaskKiller taskKiller;

    // Initialized at start of registration:
    private SchedulerDriver driver;
    // Initialized when registration completes:
    private PlanManager planManager;
    private PlanScheduler planScheduler;
    private CassandraRecoveryScheduler recoveryScheduler;
    private Collection<Object> resources;

    @Inject
    public CassandraScheduler(
            final ConfigurationManager configurationManager,
            final MesosConfig mesosConfig,
            final PersistentOfferRequirementProvider offerRequirementProvider,
            final CassandraState cassandraState,
            final SchedulerClient client,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair,
            final UpgradeSSTableManager upgrade,
            @Named("ConfiguredEnableUpgradeSSTableEndpoint") boolean enableUpgradeSSTableEndpoint,
            final SeedsManager seeds,
            final ScheduledExecutorService executor,
            final StateStore stateStore,
            final DefaultConfigurationManager defaultConfigurationManager,
            final Capabilities capabilities) {
        this.mesosConfig = mesosConfig;
        this.cassandraState = cassandraState;
        this.reconciler = new DefaultReconciler(stateStore);
        this.configurationManager = configurationManager;
        this.offerRequirementProvider = offerRequirementProvider;
        offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new PersistentOperationRecorder(cassandraState)));
        recoveryScheduler = new CassandraRecoveryScheduler(
                offerRequirementProvider, offerAccepter, cassandraState);
        recoveryScheduler.subscribe(this);
        this.client = client;
        this.backup = backup;
        this.restore = restore;
        this.cleanup = cleanup;
        this.repair = repair;
        this.upgrade = upgrade;
        this.enableUpgradeSSTableEndpoint = enableUpgradeSSTableEndpoint;
        this.seeds = seeds;
        this.executor = executor;
        this.stateStore = stateStore;
        this.defaultConfigurationManager = defaultConfigurationManager;
        this.capabilities = capabilities;

        this.offerFilters = Protos.Filters.newBuilder().setRefuseSeconds(mesosConfig.getRefuseSeconds()).build();
        LOGGER.info("Creating an offer filter with refuse_seconds = {}", mesosConfig.getRefuseSeconds());
    }

    @Override
    public void registered(SchedulerDriver driver,
                           Protos.FrameworkID frameworkId,
                           Protos.MasterInfo masterInfo) {
        // Call re-register and return early if registered() has already been called once
        // in the lifetime of this object.
        if (!isSchedulerRegistered.compareAndSet(false, true)) {
            LOGGER.info("Scheduler is already registered, calling reregistered()");
            reregistered(driver, masterInfo);
            return;
        }

        // The following is executed only once in the lifetime of this object.
        final String frameworkIdValue = frameworkId.getValue();
        LOGGER.info("Framework registered : id = {}", frameworkIdValue);
        try {
            CassandraScheduler.taskKiller = new DefaultTaskKiller(
                    stateStore,
                    new DefaultTaskFailureListener(stateStore),
                    driver);
            this.planScheduler = new DefaultPlanScheduler(offerAccepter, new OfferEvaluator(stateStore), taskKiller);
            stateStore.storeFrameworkId(frameworkId);
            Plan plan = new CassandraPlan(
                    defaultConfigurationManager,
                    ReconciliationPhase.create(reconciler),
                    SyncDataCenterPhase.create(
                            new SeedsManager(
                                    defaultConfigurationManager, cassandraState, executor, client, stateStore),
                            executor),
                    CassandraDaemonPhase.create(
                            cassandraState, offerRequirementProvider, client, defaultConfigurationManager),
                    Arrays.asList(
                            backup, restore, cleanup, repair, upgrade));
            plan.subscribe(this);
            planManager = new DefaultPlanManager(plan);
            reconciler.start();
            suppressOrRevive();
            // use add() to just throw if full:
            resourcesQueue.add(Arrays.asList(
                    new ServiceConfigResource(configurationManager),
                    new SeedsResource(seeds),
                    new ConfigurationResource(defaultConfigurationManager),
                    new TasksResource(capabilities, cassandraState, client, configurationManager),
                    new PlansResource(ImmutableMap.of("deploy", planManager)), // TODO(nick) include recovery
                    new BackupResource(backup),
                    new RestoreResource(restore),
                    new CleanupResource(cleanup),
                    new RepairResource(repair),
                    new UpgradeSSTableResource(upgrade, enableUpgradeSSTableEndpoint),
                    new DataCenterResource(seeds),
                    new ConnectionResource(capabilities, cassandraState, configurationManager),
                    // TODO(nick) rename upstream to StringPropertyDeserializer:
                    new StateResource(stateStore, new JsonPropertyDeserializer())));
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
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        if (offers == null) {
            return;
        }
        LOGGER.info("Received {} offers", offers.size());
        for (Protos.Offer offer : offers) {
            LOGGER.info("Received Offer: {}", TextFormat.shortDebugString(offer));
        }

        //TODO(nick): Switch to PlanCoordinator (after switching to DefaultRecoveryPlanManager)

        // 1. reconciliation
        reconciler.reconcile(driver);

        try {
            final List<Protos.OfferID> acceptedOffers = new ArrayList<>();

            // 2. deployment
            final Collection<? extends Step> currentSteps = planManager.getCandidates(Collections.emptyList());
            LOGGER.info("Current execution steps = {}", currentSteps);
            if (!currentSteps.isEmpty()) {
                try {
                    acceptedOffers.addAll(planScheduler.resourceOffers(driver, offers, currentSteps));
                } catch (Throwable t) {
                    LOGGER.error("Error occurred with plan scheduler:", t);
                }
            }

            // 3. recovery
            final List<Protos.Offer> unacceptedOffers = filterAcceptedOffers(offers, acceptedOffers);
            try {
                acceptedOffers.addAll(recoveryScheduler.resourceOffers(
                        driver,
                        unacceptedOffers,
                        currentSteps.stream()
                                .map(step -> step.getName())
                                .collect(Collectors.toSet())));
            } catch (Throwable t) {
                LOGGER.error("Error occured with recovery scheduler:", t);
            }

            // 4. cleanup
            ResourceCleanerScheduler cleanerScheduler = getCleanerScheduler();
            if (cleanerScheduler != null) {
                try {
                    acceptedOffers.addAll(cleanerScheduler.resourceOffers(driver, offers));
                } catch (Throwable t) {
                    LOGGER.error("Error occured with cleaner scheduler:", t);
                }
            }

            // 5. decline
            for (Protos.Offer offer : offers) {
                Protos.OfferID offerId = offer.getId();
                if (!acceptedOffers.contains(offerId)) {
                    LOGGER.info("Scheduler declining offer: {}", TextFormat.shortDebugString(offerId));
                    driver.declineOffer(offerId, offerFilters);
                }
            }
        } catch (Throwable t) {
            LOGGER.error("Error in offer acceptance cycle", t);
        }
    }

    private ResourceCleanerScheduler getCleanerScheduler() {
        ResourceCleaner cleaner;
        try {
            cleaner = new ResourceCleaner(stateStore);
        } catch (Exception ex) {
            LOGGER.error("Failed to construct ResourceCleaner with exception:", ex);
            return null;
        }
        return new ResourceCleanerScheduler(cleaner, offerAccepter);
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
            LOGGER.error("Error updating Plan Manager with status: {} reason: {}", status, ex);
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

    public void registerFramework() throws IOException {
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

    @VisibleForTesting
    void setSchedulerDriver(SchedulerDriver driver) {
        this.driver = driver;
    }

    @VisibleForTesting
    PlanManager getPlanManager() {
        return planManager;
    }

    @VisibleForTesting
    Reconciler getReconciler() {
        return reconciler;
    }

    @Override
    public void update(Observable observable) {
        if (observable == planManager.getPlan() ||
            observable == recoveryScheduler) {
            suppressOrRevive();
        }
    }

    private void suppressOrRevive() {
        boolean hasOperations = !planManager.getPlan().isComplete() ||
                recoveryScheduler.hasOperations();
        LOGGER.debug(hasOperations ?
                "Scheduler has operations to perform." :
                "Scheduler has no operations to perform.");
        if (hasOperations) {
            // Revive offers only if they were previously suppressed.
            if (cassandraState.isSuppressed()) {
                LOGGER.info("Reviving offers.");
                driver.reviveOffers();
                cassandraState.setSuppressed(false);
            }
        } else {
            LOGGER.info("Suppressing offers.");
            driver.suppressOffers();
            cassandraState.setSuppressed(true);
        }
    }

    public static TaskKiller getTaskKiller() {
        return taskKiller;
    }

    Collection<Object> getResources() throws InterruptedException {
        if (resources == null) {
            // wait for scheduler thread to register and create resources
            LOGGER.info("Waiting for registration to finish and resources to become available...");
            resources = resourcesQueue.poll(60, TimeUnit.SECONDS);
            if (resources == null) {
                throw new RuntimeException("Timed out waiting for resources from scheduler");
            }
            LOGGER.info("Got {} resources", resources.size());
        }
        return resources;
    }
}
