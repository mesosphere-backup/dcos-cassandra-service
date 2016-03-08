package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
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

public class RestoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreManager.class);

    public static final String RESTORE_KEY = "restore";

    private RestoreStage stage;
    private StageManager stageManager;
    private StageScheduler stageScheduler;
    private OfferAccepter offerAccepter;
    private CassandraTasks cassandraTasks;
    private volatile RestoreContext context;
    private ConfigurationManager configurationManager;
    private ClusterTaskOfferRequirementProvider provider;
    private PersistentReference<RestoreContext> persistentContext;
    private final CassandraPhaseStrategies phaseStrategies;

    private Block getCurrentBlock(){
        return (stageManager == null) ? null : stageManager.getCurrentBlock();
    }

    @Inject
    public RestoreManager(ConfigurationManager configurationManager,
                          CassandraTasks cassandraTasks,
                          ClusterTaskOfferRequirementProvider provider,
                          PersistenceFactory persistenceFactory,
                          CassandraPhaseStrategies phaseStrategies,
                          final Serializer<RestoreContext> serializer) {
        this.provider = provider;
        this.cassandraTasks = cassandraTasks;
        this.configurationManager = configurationManager;
        this.phaseStrategies = phaseStrategies;

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

        if (this.stageManager != null) {
            if (this.stageManager.isComplete()) {
                this.stopRestore();
            }
        }

        List<Protos.OfferID> acceptedOffers = new ArrayList<>();
        final Block currentBlock = stageManager.getCurrentBlock();

        // Nothing to schedule
        if (currentBlock == null) {
            LOGGER.info("Nothing to schedule as current block is null: {}", currentBlock);
            return acceptedOffers;
        }


        final String daemon =
                ((AbstractClusterTaskBlock)currentBlock).getDaemon();
        final CassandraDaemonTask task = cassandraTasks.getDaemons().get
                (daemon);
        if(daemon == null){
            return acceptedOffers;
        }

        LOGGER.info("RestoreManager found next block to be scheduled: {}", currentBlock);

        // Find the offer from slave on which we the cassandra daemon is running for this block.
        final String slaveId = task.getSlaveId();
        List<Protos.Offer> chosenOne = new ArrayList<>(1);
        for (Protos.Offer offer : offers) {
            if (offer.getSlaveId().getValue().equals(slaveId)) {
                LOGGER.info(
                        "Found slave on which the cassandra daemon is running: {}",
                        slaveId);
                chosenOne.add(offer);
                break;
            }
        }

        acceptedOffers.addAll(
                stageScheduler.resourceOffers(driver, chosenOne, currentBlock));

        LOGGER.info("RestoreManager accepted following offers: {}", acceptedOffers);

        return acceptedOffers;
    }

    public void startRestore(RestoreContext context) {
        LOGGER.info("Starting restore");

        this.offerAccepter = new OfferAccepter(Arrays.asList(
                new LogOperationRecorder(),
                new PersistentOperationRecorder(cassandraTasks)));
        final int servers = configurationManager.getServers();
        this.stage = new RestoreStage(context, cassandraTasks, provider);

        // TODO: Make install strategy pluggable
        this.stageManager = new DefaultStageManager(stage,phaseStrategies);
        this.stageScheduler = new DefaultStageScheduler(offerAccepter);

        try {
            this.context = context;
            persistentContext.store(this.context);
        } catch (PersistenceException e) {
            LOGGER.error("Error storing restore context into persistence store. Reason: ", e);
            throw new RuntimeException(e);
        }
    }

    public void update(Protos.TaskStatus status){
        Block block = getCurrentBlock();
        if(block != null){
            block.update(status);
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

    public RestoreStage getRestoreStage() {
        return this.stage;
    }

    public StageManager getStageManager() {return this.stageManager;}
}
