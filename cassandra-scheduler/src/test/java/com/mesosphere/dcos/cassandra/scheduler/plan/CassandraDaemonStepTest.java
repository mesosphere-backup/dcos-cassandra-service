package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.metrics.StatsDMetrics;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.scheduler.TestUtils;
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
import org.apache.mesos.offer.TaskUtils;
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
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CassandraDaemonStepTest {
    @Mock
    private PersistentOfferRequirementProvider persistentOfferRequirementProvider;
    @Mock
    private CassandraState cassandraState;
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
    private static MetricConfig metricConfig;
    private static StatsDMetrics metrics;

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

        metricConfig = config.getMetricConfig();
        metrics = new StatsDMetrics(metricConfig);

        cassandraState = new CassandraState(
                new ConfigurationManager(taskFactory, configurationManager),
                clusterTaskConfig,
                stateStore,
                metrics);
    }

    @Test
    public void testCreate() throws Exception {
        final CassandraContainer cassandraContainer = mock(CassandraContainer.class);
        final CassandraDaemonTask daemonTask = mock(CassandraDaemonTask.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraContainer.getDaemonTask()).thenReturn(daemonTask);
        when(daemonTask.getName()).thenReturn(EXPECTED_NAME);
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        Assert.assertEquals(EXPECTED_NAME, step.getName());
        Assert.assertEquals(Status.PENDING, step.getStatus());
    }

    @Test
    public void testStart() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        final Optional<OfferRequirement> offerRequirement = step.start();
        Assert.assertTrue(offerRequirement.isPresent());
    }

    @Test
    public void testStartCompleted() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                String.valueOf(configurationManager.getTargetName()),
                CassandraTaskExecutor.create(
                        configurationManager.getTargetName().toString(),
                        EXPECTED_NAME,
                        "cassandra-role",
                        "cassandra-principal",
                        config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(TaskUtils.packTaskInfo(taskInfo)));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        stateStore.storeStatus(status);

        Assert.assertTrue(!step.start().isPresent());
    }

    @Test
    public void testStartNeedConfigUpdateNotTerminated1() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                String.valueOf(configurationManager.getTargetName()),
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(TaskUtils.packTaskInfo(taskInfo)));

        Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        stateStore.storeStatus(status);

        Assert.assertTrue(!step.start().isPresent());

        when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(true);

        status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FINISHED);
        stateStore.storeStatus(status);

        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(Mockito.any())).thenReturn(
                Optional.empty());
        Assert.assertTrue(!step.start().isPresent());
    }

    @Test
    public void testStartNeedConfigUpdateNotTerminated2() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                String.valueOf(configurationManager.getTargetName()),
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(TaskUtils.packTaskInfo(taskInfo)));

        Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        stateStore.storeStatus(status);

        Assert.assertTrue(!step.start().isPresent());
        when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(false);

        status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FINISHED);
        stateStore.storeStatus(status);

        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(Mockito.any())).thenReturn(
                Optional.empty());
        Assert.assertTrue(!step.start().isPresent());
    }

    @Test
    public void testStartNeedConfigUpdateTerminated() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(TaskUtils.packTaskInfo(taskInfo)));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FINISHED);
        stateStore.storeStatus(status);
        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(any())).thenReturn(
                Optional.of(mockOfferReq));
        Assert.assertTrue(step.start().isPresent());
    }

    @Test
    public void testStartTerminated() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(EXPECTED_NAME,
                persistentOfferRequirementProvider, cassandraState);
        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        String target = new ConfigurationManager(taskFactory, configurationManager).getTargetConfigName().toString();
        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                target,
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal",
                        config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(TaskUtils.packTaskInfo(taskInfo)));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_FAILED);
        stateStore.storeStatus(status);
        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(any())).thenReturn(
                Optional.of(mockOfferReq));
        OfferRequirement offerRequirement = step.start().get();
        Assert.assertEquals(mockOfferReq, offerRequirement);

        CassandraTemplateTask templateTask = cassandraState.getOrCreateTemplateTask(
                CassandraTemplateTask.toTemplateTaskName(task.getName()), task);
        ArgumentCaptor<CassandraContainer> containerCaptor = ArgumentCaptor.forClass(CassandraContainer.class);
        verify(persistentOfferRequirementProvider).getReplacementOfferRequirement(containerCaptor.capture());
        Assert.assertEquals(templateTask, Whitebox.getInternalState(containerCaptor.getValue(), "clusterTemplateTask"));
    }

    @Test
    public void testStartLaunching() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                configurationManager.getTargetName().toString(),
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(TaskUtils.packTaskInfo(taskInfo)));

        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_STAGING);
        stateStore.storeStatus(status);


        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(any())).thenReturn(
                Optional.empty());
        Assert.assertTrue(!step.start().isPresent());

        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(any())).thenReturn(
                Optional.of(mockOfferReq));
        Assert.assertEquals(mockOfferReq, step.start().get());
    }

    @Test
    public void testUpdateDataPresent() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));
        step.updateOfferStatus(Collections.emptyList());
        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);

        step.update(status);
    }

    @Test
    public void testUpdateDataNotPresent() throws Exception {
        final String EXPECTED_NAME = "node-0";
        CassandraDaemonStep step = CassandraDaemonStep.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraState);

        final OfferRequirement mockOfferReq = mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(
                Optional.of(mockOfferReq));

        Assert.assertTrue(step.start().isPresent());

        final CassandraDaemonTask task = taskFactory.create(EXPECTED_NAME,
                "abc",
                CassandraTaskExecutor.create("1234", EXPECTED_NAME, "cassandra-role", "cassandra-principal", config.getExecutorConfig()),
                config.getCassandraConfig());
        Protos.TaskInfo taskInfo = task.getTaskInfo();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("1.2.3.4").build()).build();
        stateStore.storeTasks(Arrays.asList(taskInfo));
        step.updateOfferStatus(Collections.emptyList());
        final Protos.TaskStatus status = TestUtils.generateStatus(taskInfo.getTaskId(),
                Protos.TaskState.TASK_RUNNING);

        step.update(status);
    }
}
