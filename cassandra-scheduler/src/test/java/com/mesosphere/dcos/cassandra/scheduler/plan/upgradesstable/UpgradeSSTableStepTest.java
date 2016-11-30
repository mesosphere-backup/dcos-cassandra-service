package com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable;

import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableTask;
import com.mesosphere.dcos.cassandra.scheduler.TestUtils;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.scheduler.plan.Status;
import org.apache.mesos.state.StateStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

public class UpgradeSSTableStepTest {
    public static final String UPGRADESSTABLE_NODE_0 = "upgradesstable-node-0";
    public static final String NODE_0 = "node-0";
    @Mock
    private ClusterTaskOfferRequirementProvider provider;
    @Mock
    private CassandraState cassandraState;
    @Mock
    private SchedulerClient client;
    public static final UpgradeSSTableContext CONTEXT = UpgradeSSTableContext.create(Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList());

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        final StateStore mockStateStore = Mockito.mock(StateStore.class);
        final Protos.TaskStatus status = TestUtils
                .generateStatus(TaskUtils.toTaskId("node-0"), Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        Mockito.when(mockStateStore.fetchStatus("node-0")).thenReturn(Optional.of(status));
        Mockito.when(cassandraState.getStateStore()).thenReturn(mockStateStore);
    }

    @Test
    public void testInitial() {
        Mockito.when(cassandraState.get(UPGRADESSTABLE_NODE_0)).thenReturn(Optional.empty());
        final UpgradeSSTableContext context = UpgradeSSTableContext.create(Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
        final UpgradeSSTableStep step = new UpgradeSSTableStep(NODE_0, cassandraState, provider, context);
        Assert.assertEquals(UPGRADESSTABLE_NODE_0, step.getName());
        Assert.assertEquals(NODE_0, step.getDaemon());
        Assert.assertTrue(step.isPending());
    }

    @Test
    public void testComplete() {
        final CassandraTask mockCassandraTask = Mockito.mock(CassandraTask.class);
        Mockito.when(mockCassandraTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        Mockito.when(cassandraState.get(UPGRADESSTABLE_NODE_0))
                .thenReturn(Optional.ofNullable(mockCassandraTask));
        final UpgradeSSTableContext context = UpgradeSSTableContext.create(Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
        final UpgradeSSTableStep step = new UpgradeSSTableStep(NODE_0, cassandraState, provider, context);
        Assert.assertEquals(UPGRADESSTABLE_NODE_0, step.getName());
        Assert.assertEquals(NODE_0, step.getDaemon());
        Assert.assertTrue(step.isComplete());
    }

    @Test
    public void testTaskStartAlreadyCompleted() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraState.get(UPGRADESSTABLE_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, null);
        Mockito.when(cassandraState.getDaemons()).thenReturn(map);
        final UpgradeSSTableContext context = UpgradeSSTableContext.create(Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());

        final UpgradeSSTableTask task = Mockito.mock(UpgradeSSTableTask.class);
        Mockito.when(task.getSlaveId()).thenReturn("1234");
        Mockito.when(cassandraState.getOrCreateUpgradeSSTable(daemonTask, CONTEXT)).thenReturn(task);

        final UpgradeSSTableStep step = new UpgradeSSTableStep(NODE_0, cassandraState, provider, context);
        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any(), Mockito.any())).thenReturn(requirement);
        Assert.assertTrue(!step.start().isPresent());
        Assert.assertTrue(step.isComplete());
    }

    @Test
    public void testTaskStart() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraState.get(UPGRADESSTABLE_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        Mockito.when(cassandraState.getDaemons()).thenReturn(map);

        final UpgradeSSTableTask task = Mockito.mock(UpgradeSSTableTask.class);
        Mockito.when(task.getSlaveId()).thenReturn("1234");
        Mockito.when(task.getType()).thenReturn(CassandraTask.TYPE.CLEANUP);
        Mockito.when(cassandraState.getOrCreateUpgradeSSTable(daemonTask, CONTEXT)).thenReturn(task);

        final UpgradeSSTableStep step = new UpgradeSSTableStep(NODE_0, cassandraState, provider, CONTEXT);
        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any(), Mockito.any())).thenReturn(requirement);
        Assert.assertTrue(step.start().isPresent());
        // not IN_PROGRESS until the requirement is fulfilled!:
        Assert.assertEquals(Status.PENDING, step.getStatus());
    }
}
