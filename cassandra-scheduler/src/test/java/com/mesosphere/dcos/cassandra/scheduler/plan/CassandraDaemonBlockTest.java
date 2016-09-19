package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.scheduler.TestUtils;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.apache.mesos.state.StateStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CassandraDaemonBlockTest {
    @Mock
    private PersistentOfferRequirementProvider persistentOfferRequirementProvider;
    @Mock
    private CassandraTasks cassandraTasks;
    @Mock
    private SchedulerClient client;
    @Mock
    private CompletionStage<Boolean> mockStage;
    @Mock
    private CompletableFuture<Boolean> mockFuture;

    private static TestingServer server;
    private static CassandraDaemonTask.Factory taskFactory;
    private static MutableSchedulerConfiguration config;
    private static ClusterTaskConfig clusterTaskConfig;
    private static StateStore stateStore;
    private static DefaultConfigurationManager configurationManager;

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        server = new TestingServer();
        server.start();

        Capabilities mockCapabilities = mock(Capabilities.class);
        when(mockCapabilities.supportsNamedVips()).thenReturn(true);
        taskFactory = new CassandraDaemonTask.Factory(mockCapabilities);

        final ConfigurationFactory<MutableSchedulerConfiguration> factory =
                new ConfigurationFactory<>(
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
                Resources.getResource("scheduler.yml").getFile());

        final CassandraSchedulerConfiguration targetConfig = config.createConfig();
        clusterTaskConfig = targetConfig.getClusterTaskConfig();

        final CuratorFrameworkConfig curatorConfig = config.getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        stateStore = new CuratorStateStore(
                targetConfig.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        stateStore.storeFrameworkId(Protos.FrameworkID.newBuilder().setValue("1234").build());
        configurationManager = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                        config.createConfig().getServiceConfig().getName(),
                        server.getConnectString(),
                        config.createConfig(),
                        new ConfigValidator(),
                        stateStore);

        cassandraTasks = new CassandraTasks(
                new ConfigurationManager(taskFactory, configurationManager),
                curatorConfig,
                clusterTaskConfig,
                stateStore);
    }

    @Test
    public void testCreate() throws Exception {
        final CassandraContainer cassandraContainer = mock(CassandraContainer.class);
        final CassandraDaemonTask daemonTask = mock(CassandraDaemonTask.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraContainer.getDaemonTask()).thenReturn(daemonTask);
        when(daemonTask.getName()).thenReturn(EXPECTED_NAME);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        Assert.assertEquals(EXPECTED_NAME, block.getName());
        Assert.assertEquals(Status.PENDING, Block.getStatus(block));
    }

    @Test
    public void testStart() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        final OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);
    }

    @Test
    public void testStartCompleted() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        stateStore.storeStatus(status);

        offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
    }

    @Test
    public void testStartCompletedReconcile() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(), Protos.TaskState.TASK_RUNNING);
        stateStore.storeStatus(status);

        offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
    }

    @Test
    public void testStartNeedConfigUpdateNotTerminated1() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));

        Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        stateStore.storeStatus(status);

        offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
        when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(true);
        when(client.shutdown("1234", 1234)).thenReturn(mockStage);

        status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FINISHED);
        stateStore.storeStatus(status);

        offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
    }

    @Test
    public void testStartNeedConfigUpdateNotTerminated2() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));

        Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        stateStore.storeStatus(status);

        offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
        when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(false);
        when(client.shutdown("1234", 1234)).thenReturn(mockStage);

        status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FINISHED);
        stateStore.storeStatus(status);

        offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
    }

    @Test
    public void testStartNeedConfigUpdateTerminated() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FINISHED);
        stateStore.storeStatus(status);
        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(any())).thenReturn(mockOfferReq);
        Assert.assertNotNull(block.start());
    }

    @Test
    public void testStartTerminated() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(EXPECTED_NAME,
                persistentOfferRequirementProvider, cassandraTasks, client);
        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        String target = new ConfigurationManager(taskFactory, configurationManager).getTargetConfigName().toString();
        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                target,
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal",
                        config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FAILED);
        stateStore.storeStatus(status);
        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(any())).thenReturn(mockOfferReq);
        offerRequirement = block.start();
        Assert.assertEquals(mockOfferReq, offerRequirement);

        CassandraTemplateTask templateTask = cassandraTasks.getOrCreateTemplateTask(
                CassandraTemplateTask.toTemplateTaskName(task.getName()), task);
        ArgumentCaptor<CassandraContainer> containerCaptor = ArgumentCaptor.forClass(CassandraContainer.class);
        verify(persistentOfferRequirementProvider).getReplacementOfferRequirement(containerCaptor.capture());
        Assert.assertEquals(templateTask, Whitebox.getInternalState(containerCaptor.getValue(), "clusterTemplateTask"));
    }

    @Test
    public void start_whenContainerTerminated_shouldUseExistingTemplateTask() throws Exception {
        String nodeName = "node-2";
        CassandraContainer container = mock(CassandraContainer.class);
        when(container.getAgentId()).thenReturn("agent-2");
        when(container.isTerminated()).thenReturn(true);

        CassandraTasks tasks = mock(CassandraTasks.class);
        when(tasks.getOrCreateContainer(nodeName)).thenReturn(container);

        CassandraDaemonTask daemonTask = taskFactory.create(
                nodeName,
                "abc",
                CassandraTaskExecutor.create("1234", nodeName, "role", "principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        when(container.getDaemonTask()).thenReturn(daemonTask);
        Protos.TaskInfo taskInfo = daemonTask.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build())
                .build();
        Protos.TaskStatus taskStatus = TestUtils.generateStatus(taskInfo.getTaskId(), Protos.TaskState.TASK_ERROR);
        StateStore stateStore = mock(StateStore.class);
        when(stateStore.fetchStatus(anyString())).thenReturn(taskStatus);
        when(tasks.getStateStore()).thenReturn(stateStore);

        CassandraTemplateTask templateTask = mock(CassandraTemplateTask.class);
        when(tasks.getOrCreateTemplateTask(nodeName + "-task-template", daemonTask)).thenReturn(templateTask);

        CassandraDaemonTask newDaemonTask = mock(CassandraDaemonTask.class);
        when(tasks.replaceDaemon(daemonTask)).thenReturn(newDaemonTask);
        when(tasks.createCassandraContainer(newDaemonTask, templateTask)).thenReturn(container);

        CassandraDaemonBlock block = CassandraDaemonBlock.create(nodeName,
                persistentOfferRequirementProvider, tasks, client);
        block.start();

        verify(persistentOfferRequirementProvider).getReplacementOfferRequirement(container);
        verify(tasks).createCassandraContainer(newDaemonTask, templateTask);
    }

    @Test
    public void testStartLaunching() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                configurationManager.getTargetName().toString(),
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_STAGING);
        stateStore.storeStatus(status);

        offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(any())).thenReturn(mockOfferReq);

        Assert.assertEquals(mockOfferReq, block.start());
    }

    @Test
    public void testUpdateDataPresent() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));
        block.updateOfferStatus(true);
        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);

        block.update(status);
    }

    @Test
    public void testUpdateDataNotPresent() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));
        block.updateOfferStatus(true);
        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING);

        block.update(status);
    }
}
