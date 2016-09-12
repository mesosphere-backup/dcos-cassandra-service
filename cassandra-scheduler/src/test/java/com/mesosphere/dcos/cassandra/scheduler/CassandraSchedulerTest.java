package com.mesosphere.dcos.cassandra.scheduler;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTemplateTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPhaseStrategies;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraStageManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.ReconciliationPhase;
import org.apache.mesos.scheduler.plan.StageManager;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.testing.QueuedSchedulerDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class CassandraSchedulerTest {
    @Mock private CompletionStage<Boolean> mockStage;
    @Mock private CompletableFuture<Boolean> mockFuture;

    private CassandraScheduler scheduler;
    private ConfigurationManager configurationManager;
    private StageManager stageManager;
    private PersistentOfferRequirementProvider offerRequirementProvider;
    private CassandraTasks cassandraTasks;
    private Reconciler reconciler;
    private EventBus eventBus;
    private SchedulerClient client;
    private BackupManager backup;
    private RestoreManager restore;
    private CleanupManager cleanup;
    private RepairManager repair;
    private SeedsManager seeds;
    private ExecutorService executorService;
    private MesosConfig mesosConfig;
    private Protos.FrameworkID frameworkId;
    private Protos.MasterInfo masterInfo;
    private StateStore stateStore;
    private static MutableSchedulerConfiguration config;
    private static TestingServer server;
    private QueuedSchedulerDriver driver;
    private ConfigurationFactory<MutableSchedulerConfiguration> factory;

    @Before
    public void beforeEach() throws Exception {
        server = new TestingServer();
        server.start();
        beforeHelper("scheduler.yml");
    }

    public void beforeHelper(String configName) throws Exception {
        MockitoAnnotations.initMocks(this);
        mesosConfig = Mockito.mock(MesosConfig.class);

        client = Mockito.mock(SchedulerClient.class);
        Mockito.when(mockFuture.get()).thenReturn(true);
        Mockito.when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        Mockito.when(client.shutdown(Mockito.anyString(), Mockito.anyInt())).thenReturn(mockStage);
        backup = Mockito.mock(BackupManager.class);
        restore = Mockito.mock(RestoreManager.class);
        cleanup = Mockito.mock(CleanupManager.class);
        repair = Mockito.mock(RepairManager.class);
        seeds = Mockito.mock(SeedsManager.class);

        executorService = Executors.newCachedThreadPool();
        frameworkId = TestUtils.generateFrameworkId();
        reconciler = new DefaultReconciler();
        eventBus = new EventBus();

        stageManager = new CassandraStageManager(
                new CassandraPhaseStrategies("org.apache.mesos.scheduler.plan.DefaultInstallStrategy"));

        factory = new ConfigurationFactory<>(
                MutableSchedulerConfiguration.class,
                BaseValidator.newValidator(),
                Jackson.newObjectMapper().registerModule(
                        new GuavaModule())
                        .registerModule(new Jdk8Module()),
                "dw");

        config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource(configName).getFile());

        final CuratorFrameworkConfig curatorConfig = config.getCuratorConfig();

        stateStore = new CuratorStateStore(
                "/" + config.getServiceConfig().getName(),
                server.getConnectString());

        DefaultConfigurationManager defaultConfigurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + config.createConfig().getServiceConfig().getName(),
                server.getConnectString(),
                config.createConfig(),
                new ConfigValidator(),
                stateStore);


        Capabilities mockCapabilities = Mockito.mock(Capabilities.class);
        when(mockCapabilities.supportsNamedVips()).thenReturn(true);
        configurationManager = new ConfigurationManager(
                new CassandraDaemonTask.Factory(mockCapabilities),
                defaultConfigurationManager);

        final ClusterTaskConfig clusterTaskConfig = configurationManager.getTargetConfig().getClusterTaskConfig();
        cassandraTasks = new CassandraTasks(
                configurationManager,
                curatorConfig,
                clusterTaskConfig,
                stateStore);

        offerRequirementProvider = new PersistentOfferRequirementProvider(defaultConfigurationManager, cassandraTasks);
        scheduler = new CassandraScheduler(
                configurationManager,
                mesosConfig,
                offerRequirementProvider,
                stageManager,
                cassandraTasks,
                reconciler,
                client,
                eventBus,
                backup,
                restore,
                cleanup,
                repair,
                seeds,
                executorService,
                stateStore,
                defaultConfigurationManager);

        masterInfo = TestUtils.generateMasterInfo();

        scheduler.registered(null, frameworkId, masterInfo);
        driver = new QueuedSchedulerDriver();
    }

    @Test
    public void testRegistered() throws Exception {
        scheduler.registered(driver, frameworkId, masterInfo);
        final Protos.FrameworkID frameworkID = stateStore.fetchFrameworkId();
        assertEquals(frameworkID, this.frameworkId);
        final Phase currentPhase = stageManager.getCurrentPhase();
        assertTrue(currentPhase instanceof ReconciliationPhase);
        assertEquals(1, currentPhase.getBlocks().size());
    }

    @Test
    public void install() {
        scheduler.registered(driver, frameworkId, Protos.MasterInfo.getDefaultInstance());
        runReconcile(driver);
        final Phase currentPhase = stageManager.getCurrentPhase();
        assertEquals("Deploy", currentPhase.getName());
        assertEquals(3, currentPhase.getBlocks().size());

        Block currentBlock = stageManager.getCurrentBlock();
        assertEquals("node-0", currentBlock.getName());
        assertTrue("expected current block to be in progress due to offer carried over in reconcile stage",
                currentBlock.isInProgress());
        Collection<QueuedSchedulerDriver.OfferOperations> offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        Collection<Protos.Offer.Operation> ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        assertTrue(currentBlock.isComplete());

        final Protos.Offer offer2 = TestUtils.generateOffer(frameworkId.getValue(), 4, 10240, 10240);
        scheduler.resourceOffers(driver, Arrays.asList(offer2));
        currentBlock = stageManager.getCurrentBlock();
        assertEquals("node-1", currentBlock.getName());
        assertTrue("expected current block to be in progress due to offer carried over in reconcile stage",
                currentBlock.isInProgress());
        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        assertTrue(currentBlock.isComplete());

        final Protos.Offer offer3 = TestUtils.generateOffer(frameworkId.getValue(), 4, 10240, 10240);
        scheduler.resourceOffers(driver, Arrays.asList(offer3));
        currentBlock = stageManager.getCurrentBlock();
        assertEquals("node-2", currentBlock.getName());
        assertTrue("expected current block to be in progress due to offer carried over in reconcile stage",
                currentBlock.isInProgress());
        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        assertTrue(currentBlock.isComplete());
    }

    @Test
    public void installAndRecover() throws Exception {
        install();
        final Phase currentPhase = stageManager.getCurrentPhase();
        final Block currentBlock = stageManager.getCurrentBlock();

        assertNull(currentPhase);
        assertNull(currentBlock);

        final CassandraDaemonTask task = cassandraTasks.getDaemons().get("node-0");
        scheduler.statusUpdate(driver,
                TestUtils.generateStatus(task.getTaskInfo().getTaskId(), Protos.TaskState.TASK_KILLED));
        Set<Protos.TaskStatus> taskStatuses = cassandraTasks.getTaskStatuses();
        final Optional<Protos.TaskStatus> first = taskStatuses.stream().filter(status -> status.getTaskId().equals(task.getTaskInfo().getTaskId())).findFirst();

        assertEquals(Protos.TaskState.TASK_KILLED, first.get().getState());

        final CassandraTask templateTask = cassandraTasks.get("node-0-task-template").get();
        final Protos.Offer offer = TestUtils.generateReplacementOffer(frameworkId.getValue(),
                task.getTaskInfo(), templateTask.getTaskInfo());
        scheduler.resourceOffers(driver, Arrays.asList(offer));
        Collection<QueuedSchedulerDriver.OfferOperations> offerOps = driver.drainAccepted();
        assertEquals(String.format("expected accepted offer: %s", offer), 1, offerOps.size());
        Collection<Protos.Offer.Operation> ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        taskStatuses = cassandraTasks.getTaskStatuses();
        final Optional<Protos.TaskStatus> node0Status = taskStatuses.stream().filter(status -> {
            try {
                return org.apache.mesos.offer.TaskUtils.toTaskName(status.getTaskId()).equals(task.getTaskInfo().getName());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).findFirst();

        assertEquals(Protos.TaskState.TASK_RUNNING, node0Status.get().getState());
    }

    @Test
    public void installAndUpdate() throws Exception {
        install();
        Phase currentPhase = stageManager.getCurrentPhase();
        Block currentBlock = stageManager.getCurrentBlock();

        assertNull(currentPhase);
        assertNull(currentBlock);

        beforeHelper("update-scheduler.yml");

        update();
        currentPhase = stageManager.getCurrentPhase();
        currentBlock = stageManager.getCurrentBlock();

        assertNull(currentPhase);
        assertNull(currentBlock);
    }

    public void update() {
        scheduler.registered(driver, frameworkId, Protos.MasterInfo.getDefaultInstance());
        runReconcile(driver);
        final Phase currentPhase = stageManager.getCurrentPhase();
        assertEquals("Deploy", currentPhase.getName());
        assertEquals(3, currentPhase.getBlocks().size());

        Block currentBlock = stageManager.getCurrentBlock();
        assertEquals("node-0", currentBlock.getName());
        assertTrue("expected current block to be in Pending due to offer carried over in reconcile stage",
                currentBlock.isPending());
        Collection<QueuedSchedulerDriver.OfferOperations> offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                0, offerOps.size());
        Collection<Protos.Offer.Operation> ops = null;

        // Roll out node-0
        final CassandraTask node0 = cassandraTasks.get("node-0").get();
        final CassandraTask node0Template = cassandraTasks.get(CassandraTemplateTask.toTemplateTaskName("node-0")).get();
        final Protos.Offer offer1 = TestUtils.generateUpdateOffer(frameworkId.getValue(), node0.getTaskInfo(),
                node0Template.getTaskInfo(), 3, 1024, 1024);
        scheduler.resourceOffers(driver, Arrays.asList(offer1));
        Mockito.verify(client).shutdown(Mockito.anyString(), Mockito.anyInt());
        // Send TASK_KILL
        scheduler.statusUpdate(driver, TestUtils.generateStatus(node0.getTaskInfo().getTaskId(),
                Protos.TaskState.TASK_KILLED));
        scheduler.resourceOffers(driver, Arrays.asList(offer1));
        currentBlock = stageManager.getCurrentBlock();
        assertEquals("node-0", currentBlock.getName());
        assertTrue("expected current block to be in progress",
                currentBlock.isInProgress());
        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        assertTrue(currentBlock.isComplete());

        // Roll out node-1
        final CassandraTask node1 = cassandraTasks.get("node-1").get();
        final CassandraTask node1Template = cassandraTasks.get(CassandraTemplateTask.toTemplateTaskName("node-1")).get();
        final Protos.Offer offer2 = TestUtils.generateUpdateOffer(frameworkId.getValue(), node1.getTaskInfo(),
                node1Template.getTaskInfo(), 3, 1024, 1024);
        Mockito.reset(client);
        scheduler.resourceOffers(driver, Arrays.asList(offer2));
        Mockito.verify(client).shutdown(Mockito.anyString(), Mockito.anyInt());
        // Send TASK_KILL
        scheduler.statusUpdate(driver, TestUtils.generateStatus(node1.getTaskInfo().getTaskId(),
                Protos.TaskState.TASK_KILLED));
        scheduler.resourceOffers(driver, Arrays.asList(offer2));
        currentBlock = stageManager.getCurrentBlock();
        assertEquals("node-1", currentBlock.getName());
        assertTrue("expected current block to be in progress",
                currentBlock.isInProgress());
        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        assertTrue(currentBlock.isComplete());

        // Roll out node-2
        final CassandraTask node2 = cassandraTasks.get("node-2").get();
        final CassandraTask node2Template = cassandraTasks.get(CassandraTemplateTask.toTemplateTaskName("node-2")).get();
        final Protos.Offer offer3 = TestUtils.generateUpdateOffer(frameworkId.getValue(), node2.getTaskInfo(),
                node2Template.getTaskInfo(), 3, 1024, 1024);
        Mockito.reset(client);
        scheduler.resourceOffers(driver, Arrays.asList(offer3));
        Mockito.verify(client).shutdown(Mockito.anyString(), Mockito.anyInt());
        // Send TASK_KILL
        scheduler.statusUpdate(driver, TestUtils.generateStatus(node2.getTaskInfo().getTaskId(),
                Protos.TaskState.TASK_KILLED));
        scheduler.resourceOffers(driver, Arrays.asList(offer3));
        currentBlock = stageManager.getCurrentBlock();
        assertEquals("node-2", currentBlock.getName());
        assertTrue("expected current block to be in progress",
                currentBlock.isInProgress());
        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        assertTrue(currentBlock.isComplete());
    }

    public static void launchAll(
            final Collection<Protos.Offer.Operation> operations,
            final Scheduler scheduler,
            final SchedulerDriver driver) {
        operations.stream()
                .filter(op -> op.getType() == Protos.Offer.Operation.Type.LAUNCH)
                .flatMap(op -> op.getLaunch().getTaskInfosList().stream())
                .collect(Collectors.toList()).stream()
                .map(info -> CassandraDaemonTask.parse(info))
                .forEach(task -> scheduler.statusUpdate(driver, TestUtils.generateStatus(task.getTaskInfo().getTaskId(),
                        Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL)));
    }

    public void runReconcile(QueuedSchedulerDriver driver) {
        Phase currentPhase = stageManager.getCurrentPhase();
        assertTrue(currentPhase instanceof ReconciliationPhase);
        assertEquals(1, currentPhase.getBlocks().size());

        while (currentPhase.getName().equals("Reconciliation") && !currentPhase.isComplete()) {
            final Protos.Offer offer = TestUtils.generateOffer(frameworkId.getValue(), 4, 10240, 10240);
            scheduler.resourceOffers(driver, Arrays.asList(offer));
            final Collection<Protos.TaskStatus> taskStatuses = driver.drainReconciling();
            if (taskStatuses.isEmpty()) {
                // All reconciled
                cassandraTasks.getTaskStatuses().forEach(status -> scheduler.statusUpdate(driver, status));
            } else {
                taskStatuses.forEach(status -> scheduler.statusUpdate(driver, status));
            }
            currentPhase = stageManager.getCurrentPhase();

            if (currentPhase.getName().equals("Reconciliation") && !currentPhase.isComplete()) {
                final Collection<Protos.OfferID> declined = driver.drainDeclined();
                assertEquals(1, declined.size());
                assertEquals(declined.iterator().next(), offer.getId());
            }
        }
    }

    @After
    public void afterEach() throws Exception {
        server.close();
        server.stop();
    }
}
