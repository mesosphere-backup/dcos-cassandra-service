package com.mesosphere.dcos.cassandra.scheduler;

import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.config.MesosConfig;
import com.mesosphere.dcos.cassandra.scheduler.offer.LogOperationRecorder;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraStage;
import com.mesosphere.dcos.cassandra.scheduler.plan.DeploymentManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.DefaultStageScheduler;
import org.apache.mesos.scheduler.plan.StageManager;
import org.apache.mesos.scheduler.plan.StageScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class CassandraScheduler implements Scheduler, Managed {
    private final static Logger LOGGER = LoggerFactory.getLogger(
            CassandraScheduler.class);

    private MesosSchedulerDriver driver;
    private final IdentityManager identityManager;
    private final ConfigurationManager configurationManager;
    private final MesosConfig mesosConfig;
    private final StageManager stageManager;
    private final StageScheduler planScheduler;
    private final CassandraRepairScheduler repairScheduler;
    private final OfferAccepter offerAccepter;
    private final PersistentOfferRequirementProvider offerRequirementProvider;
    private final CassandraTasks cassandraTasks;
    private final Reconciler reconciler;
    private final EventBus eventBus;
    private final SchedulerClient client;
    private final BackupManager backup;
    private final RestoreManager restore;
    private final CleanupManager cleanup;
    private final RepairManager repair;
    private final SeedsManager seeds;
    private final ExecutorService executor;

    @Inject
    public CassandraScheduler(
            final ConfigurationManager configurationManager,
            final IdentityManager identityManager,
            final MesosConfig mesosConfig,
            final PersistentOfferRequirementProvider offerRequirementProvider,
            final StageManager stageManager,
            final CassandraTasks cassandraTasks,
            final Reconciler reconciler,
            final SchedulerClient client,
            final EventBus eventBus,
            final BackupManager backup,
            final RestoreManager restore,
            final CleanupManager cleanup,
            final RepairManager repair,
            final SeedsManager seeds,
            final ExecutorService executor) {
        this.eventBus = eventBus;
        this.mesosConfig = mesosConfig;
        this.cassandraTasks = cassandraTasks;
        this.identityManager = identityManager;
        this.configurationManager = configurationManager;
        this.offerRequirementProvider = offerRequirementProvider;
        offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new PersistentOperationRecorder(cassandraTasks)));
        planScheduler = new DefaultStageScheduler(offerAccepter);
        repairScheduler = new CassandraRepairScheduler(offerRequirementProvider,
                offerAccepter, cassandraTasks);
        this.client = client;
        this.stageManager = stageManager;
        this.reconciler = reconciler;
        this.backup = backup;
        this.restore = restore;
        this.cleanup = cleanup;
        this.repair = repair;
        this.seeds = seeds;
        this.executor = executor;
    }

    @Override
    public void start() throws Exception {
        registerFramework();
        eventBus.register(stageManager);
        eventBus.register(cassandraTasks);
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
            identityManager.register(frameworkIdValue);
            stageManager.setStage(CassandraStage.create(
                    configurationManager,
                    DeploymentManager.create(
                            offerRequirementProvider,
                            configurationManager,
                            cassandraTasks,
                            client,
                            reconciler,
                            seeds,
                            executor
                    ),
                    backup,
                    restore,
                    cleanup,
                    repair));
            reconciler.start(cassandraTasks.getTaskStatuses());
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
        reconciler.start(cassandraTasks.getTaskStatuses());
    }

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Protos.Offer> offers) {
        logOffers(offers);
        reconciler.reconcile(driver);

        try {
            if (identityManager.isRegistered()) {

                List<Protos.OfferID> acceptedOffers = new ArrayList<>();

                final Block currentBlock = stageManager.getCurrentBlock();

                LOGGER.info("Current execution block = {}",
                        (currentBlock != null) ? currentBlock.toString() :
                                "No block");

                if (currentBlock == null) {
                    LOGGER.info("Current plan {} interrupted.",
                            (stageManager.isInterrupted()) ? "is" : "is not");
                }
                acceptedOffers.addAll(
                        planScheduler.resourceOffers(driver, offers,
                                currentBlock));

                // Perform any required repairs
                List<Protos.Offer> unacceptedOffers = filterAcceptedOffers(
                        offers,
                        acceptedOffers);
                acceptedOffers.addAll(
                        repairScheduler.resourceOffers(
                                driver,
                                unacceptedOffers,
                                (currentBlock != null) ?
                                        ImmutableSet.of(
                                                currentBlock.getName()) :
                                        Collections.emptySet()));

                declineOffers(driver, acceptedOffers, offers);
            } else {

                LOGGER.info("Declining all offers : registered = {}, " +
                                "reconciled = {}",
                        identityManager.isRegistered(),
                        reconciler.isReconciled());
                declineOffers(driver, Collections.emptyList(), offers);
            }
        } catch (Throwable t){

            LOGGER.error("Error in offer acceptance cycle", t);
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
            cassandraTasks.update(status);
        } catch (Exception ex) {
            LOGGER.error("Error updating Tasks with status", ex);
        }
        try {
            stageManager.update(status);
        } catch (Exception ex) {
            LOGGER.error("Error updating Stage Manager with status", ex);
        }

    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 Protos.ExecutorID executorId
            ,
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
                executorId.getValue()
                , slaveId.getValue(), status);
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
        Identity identity = identityManager.get();
        Optional<ByteString> secretBytes = identity.readSecretBytes();
        if (secretBytes.isPresent()) {
            // Authenticated if a non empty secret is provided.
            Protos.Credential credential = Protos.Credential.newBuilder()
                    .setPrincipal(identity.getPrincipal())
                    .setSecretBytes(secretBytes.get())
                    .build();
            this.driver = new MesosSchedulerDriver(
                    this,
                    identity.asInfo(),
                    mesosConfig.toZooKeeperUrl(),
                    credential);
        } else {
            this.driver = new MesosSchedulerDriver(this,
                    identity.asInfo(),
                    mesosConfig.toZooKeeperUrl());

        }
        LOGGER.info("Starting driver...");
        final Protos.Status startStatus = this.driver.start();
        LOGGER.info("Driver started with status: {}", startStatus);
    }

    private void logOffers(List<Protos.Offer> offers) {
        if (Objects.isNull(offers)) {
            return;
        }

        LOGGER.info("Received {} offers", offers.size());

        for (Protos.Offer offer : offers) {
            LOGGER.debug("Received Offer: {}", offer);
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
        driver.declineOffer(offerId);
    }
}
