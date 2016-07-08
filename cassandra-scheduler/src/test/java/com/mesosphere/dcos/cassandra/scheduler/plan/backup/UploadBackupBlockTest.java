package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Status;
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
    }

    @Test
    public void testInitial() {
        Mockito.when(cassandraTasks.get(UPLOAD_NODE_0)).thenReturn(Optional.empty());
        final BackupContext context = BackupContext.create("", "", "", "", "", "");
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
        final BackupContext context = BackupContext.create("", "", "", "", "", "");
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
        final BackupContext context = BackupContext.create("", "", "", "", "", "");

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
        Assert.assertNull(block.start());
        Assert.assertTrue(block.isComplete());
    }

    @Test
    public void testTaskStart() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get(UPLOAD_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupContext context = BackupContext.create("", "", "", "", "", "");

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
        Assert.assertNotNull(block.start());
        Assert.assertTrue(block.isInProgress());
    }
}
