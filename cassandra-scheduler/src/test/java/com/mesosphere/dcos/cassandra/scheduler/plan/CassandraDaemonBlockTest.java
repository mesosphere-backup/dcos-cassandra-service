package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.protobuf.TaskStatusBuilder;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.apache.mesos.state.StateStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class CassandraDaemonBlockTest {
    @Mock
    private PersistentOfferRequirementProvider persistentOfferRequirementProvider;
    @Mock
    private CassandraTasks cassandraTasks;
    @Mock
    private SchedulerClient client;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreate() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(false);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        Assert.assertEquals(EXPECTED_NAME, block.getName());
        Assert.assertEquals(Status.Pending, Block.getStatus(block));
    }

    @Test
    public void testStart() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(false);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        when(cassandraContainer.getAgentId()).thenReturn("");
        final OfferRequirement mockOfferReq = Mockito.mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        final OfferRequirement offerRequirement = block.start();
        Assert.assertNotNull(offerRequirement);
    }

    @Test
    public void testStartCompleted() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(false);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        when(cassandraContainer.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        when(cassandraContainer.getMode()).thenReturn(CassandraMode.NORMAL);
        final OfferRequirement mockOfferReq = Mockito.mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getNewOfferRequirement(Mockito.any())).thenReturn(mockOfferReq);

        final OfferRequirement offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
    }

    @Test
    public void testStartNeedConfigUpdateNotTerminated1() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(true);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final CassandraDaemonTask mockDaemonTask = Mockito.mock(CassandraDaemonTask.class);
        when(mockDaemonTask.getHostname()).thenReturn("1234");
        final CassandraTaskExecutor mockExecutor = Mockito.mock(CassandraTaskExecutor.class);
        when(mockDaemonTask.getExecutor()).thenReturn(mockExecutor);
        when(mockExecutor.getApiPort()).thenReturn(1234);

        when(cassandraContainer.getDaemonTask()).thenReturn(mockDaemonTask);
        when(cassandraContainer.isTerminated()).thenReturn(false);
        final CompletionStage mockStage = Mockito.mock(CompletionStage.class);
        final CompletableFuture mockFuture = Mockito.mock(CompletableFuture.class);
        when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(true);
        when(client.shutdown("1234", 1234)).thenReturn(mockStage);

        final StateStore stateStore = Mockito.mock(StateStore.class);
        when(cassandraTasks.getStateStore()).thenReturn(stateStore);
        when(stateStore.fetchStatus(any()))
                .thenReturn(TaskStatusBuilder.createTaskStatus(Protos.TaskID.newBuilder().setValue("").build()
                        , Protos.TaskState.TASK_FINISHED));

        final OfferRequirement offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
    }

    @Test
    public void testStartNeedConfigUpdateNotTerminated2() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(true);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        final CassandraDaemonTask mockDaemonTask = Mockito.mock(CassandraDaemonTask.class);
        when(mockDaemonTask.getHostname()).thenReturn("1234");
        final CassandraTaskExecutor mockExecutor = Mockito.mock(CassandraTaskExecutor.class);
        when(mockDaemonTask.getExecutor()).thenReturn(mockExecutor);
        when(mockExecutor.getApiPort()).thenReturn(1234);

        when(cassandraContainer.getDaemonTask()).thenReturn(mockDaemonTask);
        when(cassandraContainer.isTerminated()).thenReturn(false);
        final CompletionStage mockStage = Mockito.mock(CompletionStage.class);
        final CompletableFuture mockFuture = Mockito.mock(CompletableFuture.class);
        when(mockStage.toCompletableFuture()).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(false);
        when(client.shutdown("1234", 1234)).thenReturn(mockStage);

        final StateStore stateStore = Mockito.mock(StateStore.class);
        when(cassandraTasks.getStateStore()).thenReturn(stateStore);
        when(stateStore.fetchStatus(any()))
                .thenReturn(TaskStatusBuilder.createTaskStatus(Protos.TaskID.newBuilder().setValue("").build()
                        , Protos.TaskState.TASK_FINISHED));

        final OfferRequirement offerRequirement = block.start();
        Assert.assertNull(offerRequirement);
    }

    @Test
    public void testStartNeedConfigUpdateTerminated() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(true);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        when(cassandraContainer.isTerminated()).thenReturn(true);
        final CassandraDaemonTask mockDaemonTask = Mockito.mock(CassandraDaemonTask.class);
        when(cassandraContainer.getDaemonTask()).thenReturn(mockDaemonTask);
        when(cassandraTasks.reconfigureDeamon(mockDaemonTask)).thenReturn(mockDaemonTask);
        final Protos.TaskInfo mockTaskInfo = Protos.TaskInfo.getDefaultInstance();
        when(mockDaemonTask.getTaskInfo()).thenReturn(mockTaskInfo);
        final OfferRequirement mockOR = Mockito.mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getUpdateOfferRequirement(mockTaskInfo)).thenReturn(mockOR);

        final StateStore stateStore = Mockito.mock(StateStore.class);
        when(cassandraTasks.getStateStore()).thenReturn(stateStore);
        when(stateStore.fetchStatus(any()))
                .thenReturn(TaskStatusBuilder.createTaskStatus(Protos.TaskID.newBuilder().setValue("").build()
                        , Protos.TaskState.TASK_FINISHED));

        Assert.assertEquals(null, block.start());
    }

    @Test
    public void testStartTerminated() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(false);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        when(cassandraContainer.getAgentId()).thenReturn("1234");
        when(cassandraContainer.isTerminated()).thenReturn(true);

        final CassandraDaemonTask mockDaemonTask = Mockito.mock(CassandraDaemonTask.class);
        when(cassandraContainer.getDaemonTask()).thenReturn(mockDaemonTask);
        when(cassandraTasks.replaceDaemon(mockDaemonTask)).thenReturn(mockDaemonTask);
        final Protos.TaskInfo mockTaskInfo = Protos.TaskInfo.getDefaultInstance();
        when(mockDaemonTask.getTaskInfo()).thenReturn(mockTaskInfo);
        final OfferRequirement mockOR = Mockito.mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(mockTaskInfo)).thenReturn(mockOR);

        Assert.assertEquals(mockOR, block.start());
    }

    @Test
    public void testStartLaunching() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        when(cassandraTasks.needsConfigUpdate(Mockito.any())).thenReturn(false);
        CassandraDaemonBlock block = CassandraDaemonBlock.create(
                EXPECTED_NAME, persistentOfferRequirementProvider, cassandraTasks, client);

        when(cassandraContainer.getAgentId()).thenReturn("1234");
        when(cassandraContainer.isTerminated()).thenReturn(false);
        when(cassandraContainer.isLaunching()).thenReturn(true);

        final CassandraDaemonTask mockDaemonTask = Mockito.mock(CassandraDaemonTask.class);
        when(cassandraContainer.getDaemonTask()).thenReturn(mockDaemonTask);
        when(cassandraTasks.replaceDaemon(mockDaemonTask)).thenReturn(mockDaemonTask);
        final Protos.TaskInfo mockTaskInfo = Protos.TaskInfo.getDefaultInstance();
        when(mockDaemonTask.getTaskInfo()).thenReturn(mockTaskInfo);
        final OfferRequirement mockOR = Mockito.mock(OfferRequirement.class);
        when(persistentOfferRequirementProvider.getReplacementOfferRequirement(mockTaskInfo)).thenReturn(mockOR);

        Assert.assertEquals(mockOR, block.start());
    }
}
