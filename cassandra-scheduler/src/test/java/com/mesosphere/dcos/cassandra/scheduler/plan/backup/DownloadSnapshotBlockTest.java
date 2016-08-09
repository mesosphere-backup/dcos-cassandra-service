package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Optional;

public class DownloadSnapshotBlockTest {
    public static final String DOWNLOAD_NODE_0 = "download-node-0";
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
        Mockito.when(cassandraTasks.get(DOWNLOAD_NODE_0)).thenReturn(Optional.empty());
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false);
        final DownloadSnapshotBlock backupSnapshotBlock = DownloadSnapshotBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                backupRestoreContext);
        Assert.assertEquals(DOWNLOAD_NODE_0, backupSnapshotBlock.getName());
        Assert.assertEquals(NODE_0, backupSnapshotBlock.getDaemon());
        Assert.assertEquals(Status.Pending, Block.getStatus(backupSnapshotBlock));
    }

    @Test
    public void testComplete() {
        final CassandraTask mockCassandraTask = Mockito.mock(CassandraTask.class);
        Mockito.when(mockCassandraTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        Mockito.when(cassandraTasks.get(DOWNLOAD_NODE_0))
                .thenReturn(Optional.ofNullable(mockCassandraTask));
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false);
        final DownloadSnapshotBlock backupSnapshotBlock = DownloadSnapshotBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                backupRestoreContext);
        Assert.assertEquals(DOWNLOAD_NODE_0, backupSnapshotBlock.getName());
        Assert.assertEquals(NODE_0, backupSnapshotBlock.getDaemon());
        Assert.assertEquals(Status.Complete, Block.getStatus(backupSnapshotBlock));
    }

    @Test
    public void testTaskStartAlreadyCompleted() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get(DOWNLOAD_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, null);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false);

        final DownloadSnapshotTask snapshotTask = Mockito.mock(DownloadSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito
                .when(cassandraTasks.getOrCreateSnapshotDownload(daemonTask, backupRestoreContext))
                .thenReturn(snapshotTask);

        final DownloadSnapshotBlock backupSnapshotBlock = DownloadSnapshotBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                backupRestoreContext);

        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any())).thenReturn(requirement);
        Assert.assertNull(backupSnapshotBlock.start());
        Assert.assertEquals(Status.Complete, Block.getStatus(backupSnapshotBlock));
    }

    @Test
    public void testTaskStart() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get(DOWNLOAD_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false);

        final DownloadSnapshotTask snapshotTask = Mockito.mock(DownloadSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito
                .when(cassandraTasks.getOrCreateSnapshotDownload(daemonTask, backupRestoreContext))
                .thenReturn(snapshotTask);

        final DownloadSnapshotBlock downloadSnapshotBlock = DownloadSnapshotBlock.create(
                NODE_0,
                cassandraTasks,
                provider,
                backupRestoreContext);

        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any())).thenReturn(requirement);
        Assert.assertNotNull(downloadSnapshotBlock.start());
        Assert.assertEquals(Status.InProgress, Block.getStatus(downloadSnapshotBlock));
    }
}
