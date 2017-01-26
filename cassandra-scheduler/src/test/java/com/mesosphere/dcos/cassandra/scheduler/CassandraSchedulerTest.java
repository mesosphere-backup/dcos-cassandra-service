package com.mesosphere.dcos.cassandra.scheduler;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable.UpgradeSSTableManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
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
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Status;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.testing.QueuedSchedulerDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class CassandraSchedulerTest {
    @Mock private CompletionStage<Boolean> mockStage;
    @Mock private CompletableFuture<Boolean> mockFuture;

    private CassandraScheduler scheduler;
    private ConfigurationManager configurationManager;
    private PersistentOfferRequirementProvider offerRequirementProvider;
    private CassandraState cassandraState;
    private SchedulerClient client;
    private BackupManager backup;
    private RestoreManager restore;
    private CleanupManager cleanup;
    private RepairManager repair;
    private UpgradeSSTableManager upgrade;
    private SeedsManager seeds;
    private Capabilities capabilities;
    private ScheduledExecutorService executorService;
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

    private void beforeHelper(String configName) throws Exception {
        MockitoAnnotations.initMocks(this);
        mesosConfig = Mockito.mock(MesosConfig.class);

        client = Mockito.mock(SchedulerClient.class);
        Mockito.when(mockFuture.get()).thenReturn(true);
        Mockito.when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        backup = Mockito.mock(BackupManager.class);
        restore = Mockito.mock(RestoreManager.class);
        cleanup = Mockito.mock(CleanupManager.class);
        repair = Mockito.mock(RepairManager.class);
        upgrade = Mockito.mock(UpgradeSSTableManager.class);
        seeds = Mockito.mock(SeedsManager.class);
        capabilities = Mockito.mock(Capabilities.class);

        executorService = Executors.newSingleThreadScheduledExecutor();
        frameworkId = TestUtils.generateFrameworkId();

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
        cassandraState = new CassandraState(
                configurationManager,
                configurationManager.getTargetConfig().getClusterTaskConfig(),
                stateStore);

        offerRequirementProvider = new PersistentOfferRequirementProvider(defaultConfigurationManager);
        scheduler = new CassandraScheduler(
                configurationManager,
                mesosConfig,
                offerRequirementProvider,
                cassandraState,
                client,
                backup,
                restore,
                cleanup,
                repair,
                upgrade,
                true,
                seeds,
                executorService,
                stateStore,
                defaultConfigurationManager,
                capabilities);

        masterInfo = TestUtils.generateMasterInfo();

        driver = new QueuedSchedulerDriver();
        scheduler.setSchedulerDriver(driver);
        scheduler.registered(driver, frameworkId, masterInfo);
    }

    @Test
    public void testRegistered() throws Exception {
        scheduler.registered(driver, frameworkId, masterInfo);
        final Protos.FrameworkID frameworkID = stateStore.fetchFrameworkId().get();
        assertEquals(frameworkID, this.frameworkId);

        Collection<? extends Step> allSteps = getAllSteps();
        assertEquals(allSteps.toString(), 4, allSteps.size());
        assertEquals("Reconciliation", allSteps.iterator().next().getName());

        Collection<? extends Step> incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 3, incompleteSteps.size());
        assertEquals("node-0", incompleteSteps.iterator().next().getName());
        assertEquals("node-0", getNextStep().getName());
    }

    @Test
    public void install() {
        scheduler.registered(driver, frameworkId, Protos.MasterInfo.getDefaultInstance());
        runReconcile(driver);

        Collection<? extends Step> incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 3, incompleteSteps.size());
        assertEquals("node-0", getNextStep().getName());

        // node-0
        final Protos.Offer offer1 = TestUtils.generateOffer(frameworkId.getValue(), 4, 10240, 10240);
        scheduler.resourceOffers(driver, Arrays.asList(offer1));

        incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 3, incompleteSteps.size());
        assertEquals("node-0", getNextStep().getName());
        assertEquals(Status.IN_PROGRESS, getNextStep().getStatus());

        Collection<QueuedSchedulerDriver.OfferOperations> offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        Collection<Protos.Offer.Operation> ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);

        assertEquals("node-1", getNextStep().getName());

        // node-1
        final Protos.Offer offer2 = TestUtils.generateOffer(frameworkId.getValue(), 4, 10240, 10240);
        scheduler.resourceOffers(driver, Arrays.asList(offer2));

        incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 2, incompleteSteps.size());
        assertEquals("node-1", getNextStep().getName());
        assertEquals(Status.IN_PROGRESS, getNextStep().getStatus());

        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);

        assertEquals("node-2", getNextStep().getName());

        // node-2
        final Protos.Offer offer3 = TestUtils.generateOffer(frameworkId.getValue(), 4, 10240, 10240);
        scheduler.resourceOffers(driver, Arrays.asList(offer3));

        incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 1, incompleteSteps.size());
        assertEquals("node-2", getNextStep().getName());
        assertEquals(Status.IN_PROGRESS, getNextStep().getStatus());

        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);

        assertTrue(getIncompleteSteps().isEmpty());
    }

    @Test
    public void installAndRecover() throws Exception {
        install();

        Collection<? extends Step> incompleteSteps = getIncompleteSteps();
        assertTrue(incompleteSteps.isEmpty());

        final CassandraDaemonTask task = cassandraState.getDaemons().get("node-0");
        scheduler.statusUpdate(driver,
                TestUtils.generateStatus(task.getTaskInfo().getTaskId(), Protos.TaskState.TASK_KILLED));
        Set<Protos.TaskStatus> taskStatuses = cassandraState.getTaskStatuses();
        final Optional<Protos.TaskStatus> first = taskStatuses.stream().filter(status -> status.getTaskId().equals(task.getTaskInfo().getTaskId())).findFirst();

        assertEquals(Protos.TaskState.TASK_KILLED, first.get().getState());

        final CassandraTask templateTask = cassandraState.get("node-0-task-template").get();
        final Protos.Offer offer = TestUtils.generateReplacementOffer(frameworkId.getValue(),
                task.getTaskInfo(), templateTask.getTaskInfo());
        scheduler.resourceOffers(driver, Arrays.asList(offer));
        Collection<QueuedSchedulerDriver.OfferOperations> offerOps = driver.drainAccepted();
        assertEquals(String.format("expected accepted offer: %s", offer), 1, offerOps.size());
        Collection<Protos.Offer.Operation> ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);
        taskStatuses = cassandraState.getTaskStatuses();
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

        Collection<? extends Step> incompleteSteps = getIncompleteSteps();
        assertTrue(incompleteSteps.isEmpty());

        beforeHelper("update-scheduler.yml");

        update();

        incompleteSteps = getIncompleteSteps();
        assertTrue(incompleteSteps.isEmpty());

        final CassandraDaemonTask task = cassandraState.getDaemons().get("node-0");
        assertTrue(task.getExecutor().getURIs().contains("https://s3-us-west-2.amazonaws.com/cassandra-framework-dev/testing/apache-cassandra-2.2.5-bin-updated.tar.gz"));
        assertEquals(0.6, task.getConfig().getCpus(), 0.0);
    }

    @Test
    public void testSuppress() throws Exception {
        assertTrue(!driver.isSuppressed());
        install();
        assertTrue(driver.isSuppressed());
    }

    @Test
    public void testRevive() throws Exception {
        install();
        assertTrue(driver.isSuppressed());
        final CassandraDaemonTask task = cassandraState.getDaemons().get("node-0");
        scheduler.statusUpdate(
                driver,
                TestUtils.generateStatus(task.getTaskInfo().getTaskId(), Protos.TaskState.TASK_KILLED));
        assertTrue(!driver.isSuppressed());
    }

    @Test
    // Test calling register() multiple times as this happens when Mesos master gets re-elected.
    public void testMultipleRegister() throws Exception {
        install();
        scheduler.registered(driver, frameworkId, masterInfo);
        scheduler.registered(driver, frameworkId, masterInfo);

        beforeHelper("update-scheduler.yml");
        update();
    }

    private void update() {
        scheduler.registered(driver, frameworkId, Protos.MasterInfo.getDefaultInstance());
        runReconcile(driver);

        Collection<? extends Step> incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 3, incompleteSteps.size());
        assertEquals("node-0", getNextStep().getName());
        assertEquals("expected current step to be in Pending due to offer carried over in reconcile stage",
                Status.PENDING, getNextStep().getStatus());

        Collection<QueuedSchedulerDriver.OfferOperations> offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                0, offerOps.size());
        Collection<Protos.Offer.Operation> ops = null;

        // Roll out node-0
        final CassandraTask node0 = cassandraState.get("node-0").get();
        final CassandraTask node0Template = cassandraState.get(CassandraTemplateTask.toTemplateTaskName("node-0")).get();
        final Protos.Offer offer1 = TestUtils.generateUpdateOffer(frameworkId.getValue(), node0.getTaskInfo(),
                node0Template.getTaskInfo(), 3, 1024, 1024);
        // Send TASK_KILL
        scheduler.statusUpdate(
                driver,
                TestUtils.generateStatus(
                        node0.getTaskInfo().getTaskId(),
                        Protos.TaskState.TASK_KILLED));
        scheduler.resourceOffers(driver, Arrays.asList(offer1));

        incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 3, incompleteSteps.size());
        assertEquals("node-0", getNextStep().getName());
        assertEquals("expected current step to be in progress",
                Status.IN_PROGRESS, getNextStep().getStatus());

        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);

        assertEquals("node-1", getNextStep().getName());

        // Roll out node-1
        final CassandraTask node1 = cassandraState.get("node-1").get();
        final CassandraTask node1Template = cassandraState.get(CassandraTemplateTask.toTemplateTaskName("node-1")).get();
        final Protos.Offer offer2 = TestUtils.generateUpdateOffer(frameworkId.getValue(), node1.getTaskInfo(),
                node1Template.getTaskInfo(), 3, 1024, 1024);
        Mockito.reset(client);
        // Send TASK_KILL
        scheduler.statusUpdate(driver, TestUtils.generateStatus(node1.getTaskInfo().getTaskId(),
                Protos.TaskState.TASK_KILLED));
        scheduler.resourceOffers(driver, Arrays.asList(offer2));

        incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 2, incompleteSteps.size());
        assertEquals("node-1", getNextStep().getName());
        assertEquals("expected current step to be in progress",
                Status.IN_PROGRESS, getNextStep().getStatus());

        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);

        assertEquals("node-2", getNextStep().getName());

        // Roll out node-2
        final CassandraTask node2 = cassandraState.get("node-2").get();
        final CassandraTask node2Template = cassandraState.get(CassandraTemplateTask.toTemplateTaskName("node-2")).get();
        final Protos.Offer offer3 = TestUtils.generateUpdateOffer(frameworkId.getValue(), node2.getTaskInfo(),
                node2Template.getTaskInfo(), 3, 1024, 1024);
        Mockito.reset(client);
        // Send TASK_KILL
        scheduler.statusUpdate(driver, TestUtils.generateStatus(node2.getTaskInfo().getTaskId(),
                Protos.TaskState.TASK_KILLED));
        scheduler.resourceOffers(driver, Arrays.asList(offer3));

        incompleteSteps = getIncompleteSteps();
        assertEquals(incompleteSteps.toString(), 1, incompleteSteps.size());
        assertEquals("node-2", getNextStep().getName());
        assertEquals("expected current step to be in progress",
                Status.IN_PROGRESS, getNextStep().getStatus());

        offerOps = driver.drainAccepted();
        assertEquals("expected accepted offer carried over from reconcile stage",
                1, offerOps.size());
        ops = offerOps.iterator().next().getOperations();
        launchAll(ops, scheduler, driver);

        assertTrue(getIncompleteSteps().isEmpty());
    }

    private static void launchAll(
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

    private void runReconcile(QueuedSchedulerDriver driver) {
        while (!scheduler.getReconciler().isReconciled()) {

            final Protos.Offer offer = TestUtils.generateOffer(frameworkId.getValue(), 4, 10240, 10240);
            scheduler.resourceOffers(driver, Arrays.asList(offer));
            final Collection<Protos.TaskStatus> taskStatuses = driver.drainReconciling();
            if (taskStatuses.isEmpty()) {
                // All reconciled
                cassandraState.getTaskStatuses().forEach(status -> scheduler.statusUpdate(driver, status));
            } else {
                taskStatuses.forEach(status -> scheduler.statusUpdate(driver, status));
            }

            if (!scheduler.getReconciler().isReconciled()) {
                final Collection<Protos.OfferID> declined = driver.drainDeclined();
                assertEquals(1, declined.size());
                assertEquals(declined.iterator().next(), offer.getId());
            }
        }
    }

    private Collection<? extends Step> getAllSteps() {
        List<Step> steps = new ArrayList<>();
        for (Phase phase : scheduler.getPlanManager().getPlan().getChildren()) {
            steps.addAll(phase.getChildren());
        }
        return steps;
    }

    private Collection<? extends Step> getIncompleteSteps() {
        List<Step> steps = new ArrayList<>();
        for (Phase phase : scheduler.getPlanManager().getPlan().getChildren()) {
            for (Step step : phase.getChildren()) {
                if (!step.isComplete()) {
                    steps.add(step);
                }
            }
        }
        return steps;
    }

    private Step getNextStep() {
        Collection<? extends Step> steps = scheduler.getPlanManager().getCandidates(Collections.emptyList());
        assertFalse(steps.toString(), steps.isEmpty());
        return steps.iterator().next();
    }

    @After
    public void afterEach() throws Exception {
        server.close();
        server.stop();
    }
}
