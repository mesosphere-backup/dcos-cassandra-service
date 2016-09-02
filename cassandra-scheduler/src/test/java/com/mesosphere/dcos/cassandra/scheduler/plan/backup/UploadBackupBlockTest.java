package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.scheduler.TestUtils;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
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

import java.util.HashMap;
import java.util.Optional;

public class UploadBackupBlockTest {
    public static final String UPLOAD_NODE_0 = "upload-node-0";
    public static final String NODE_0 = "node-0";
    @Mock
    private ClusterTaskOfferRequirementProvider provider;
    @Mock
    private CassandraTasks cassandraTasks;
    @Mock
    private SchedulerClient client;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        final StateStore mockStateStore = Mockito.mock(StateStore.class);
        final Protos.TaskStatus status = TestUtils
                .generateStatus(TaskUtils.toTaskId("node-0"), Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL);
        Mockito.when(mockStateStore.fetchStatus("node-0")).thenReturn(Optional.of(status));
        Mockito.when(cassandraTasks.getStateStore()).thenReturn(mockStateStore);
    }

    @Test
    public void testInitial() {
        Mockito.when(cassandraTasks.get(UPLOAD_NODE_0)).thenReturn(Optional.empty());
        final BackupRestoreContext context = BackupRestoreContext.create("", "", "", "", "", "", false);
        final UploadBackupBlock block = UploadBackupBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                context);
        Assert.assertEquals(UPLOAD_NODE_0, block.getName());
        Assert.assertEquals(NODE_0, block.getDaemon());
        Assert.assertTrue(block.isPending());
    }

    @Test
    public void testComplete() {
        final CassandraTask mockCassandraTask = Mockito.mock(CassandraTask.class);
        Mockito.when(mockCassandraTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        Mockito.when(cassandraTasks.get(UPLOAD_NODE_0))
                .thenReturn(Optional.ofNullable(mockCassandraTask));
        final BackupRestoreContext context = BackupRestoreContext.create("", "", "", "", "", "", false);
        final UploadBackupBlock block = UploadBackupBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                context);
        Assert.assertEquals(UPLOAD_NODE_0, block.getName());
        Assert.assertEquals(NODE_0, block.getDaemon());
        Assert.assertTrue(block.isComplete());
    }

    @Test
    public void testTaskStartAlreadyCompleted() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get(UPLOAD_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, null);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupRestoreContext context = BackupRestoreContext.create("", "", "", "", "", "", false);

        final BackupUploadTask task = Mockito.mock(BackupUploadTask.class);
        Mockito.when(task.getSlaveId()).thenReturn("1234");
        Mockito
                .when(cassandraTasks.getOrCreateBackupUpload(daemonTask, context))
                .thenReturn(task);

        final UploadBackupBlock block = UploadBackupBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                context);

        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any())).thenReturn(requirement);
        Assert.assertTrue(!block.start().isPresent());
        Assert.assertTrue(block.isComplete());
    }

    @Test
    public void testTaskStart() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get(UPLOAD_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupRestoreContext context = BackupRestoreContext.create("", "", "", "", "", "", false);

        final BackupUploadTask task = Mockito.mock(BackupUploadTask.class);
        Mockito.when(task.getSlaveId()).thenReturn("1234");
        Mockito
                .when(cassandraTasks.getOrCreateBackupUpload(daemonTask, context))
                .thenReturn(task);

        final UploadBackupBlock block = UploadBackupBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                context);

        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any())).thenReturn(requirement);
        Assert.assertTrue(block.start().isPresent());
        Assert.assertTrue(block.isInProgress());
    }
}
