package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.TestUtils;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

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

public class DownloadSnapshotStepTest {
    public static final String DOWNLOAD_NODE_0 = "download-node-0";
    public static final String NODE_0 = "node-0";
    @Mock
    private ClusterTaskOfferRequirementProvider provider;
    @Mock
    private CassandraState cassandraState;
    @Mock
    private SchedulerClient client;

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
        Mockito.when(cassandraState.get(DOWNLOAD_NODE_0)).thenReturn(Optional.empty());
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");
        final DownloadSnapshotStep downloadSnapshotStep =
                new DownloadSnapshotStep(NODE_0, cassandraState, provider, backupRestoreContext);
        Assert.assertEquals(DOWNLOAD_NODE_0, downloadSnapshotStep.getName());
        Assert.assertEquals(NODE_0, downloadSnapshotStep.getDaemon());
        Assert.assertEquals(Status.PENDING, downloadSnapshotStep.getStatus());
    }

    @Test
    public void testComplete() {
        final CassandraTask mockCassandraTask = Mockito.mock(CassandraTask.class);
        Mockito.when(mockCassandraTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        Mockito.when(cassandraState.get(DOWNLOAD_NODE_0))
                .thenReturn(Optional.ofNullable(mockCassandraTask));
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");
        final DownloadSnapshotStep downloadSnapshotStep =
                new DownloadSnapshotStep(NODE_0, cassandraState, provider, backupRestoreContext);
        Assert.assertEquals(DOWNLOAD_NODE_0, downloadSnapshotStep.getName());
        Assert.assertEquals(NODE_0, downloadSnapshotStep.getDaemon());
        Assert.assertEquals(Status.COMPLETE, downloadSnapshotStep.getStatus());
    }

    @Test
    public void testTaskStartAlreadyCompleted() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraState.get(DOWNLOAD_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, null);
        Mockito.when(cassandraState.getDaemons()).thenReturn(map);
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");

        final DownloadSnapshotTask snapshotTask = Mockito.mock(DownloadSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito.when(cassandraState.getOrCreateSnapshotDownload(daemonTask, backupRestoreContext))
                .thenReturn(snapshotTask);

        final DownloadSnapshotStep downloadSnapshotStep =
                new DownloadSnapshotStep(NODE_0, cassandraState, provider, backupRestoreContext);
        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any(), Mockito.any())).thenReturn(requirement);
        Assert.assertTrue(!downloadSnapshotStep.start().isPresent());
        Assert.assertEquals(Status.COMPLETE, downloadSnapshotStep.getStatus());
    }

    @Test
    public void testTaskStart() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraState.get(DOWNLOAD_NODE_0)).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        Mockito.when(cassandraState.getDaemons()).thenReturn(map);
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");

        final DownloadSnapshotTask snapshotTask = Mockito.mock(DownloadSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito.when(snapshotTask.getType()).thenReturn(CassandraTask.TYPE.SNAPSHOT_DOWNLOAD);
        Mockito.when(cassandraState.getOrCreateSnapshotDownload(daemonTask, backupRestoreContext))
                .thenReturn(snapshotTask);

        final DownloadSnapshotStep downloadSnapshotStep =
                new DownloadSnapshotStep(NODE_0, cassandraState, provider, backupRestoreContext);
        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any(), Mockito.any())).thenReturn(requirement);
        Assert.assertTrue(downloadSnapshotStep.start().isPresent());
        // not IN_PROGRESS until the requirement is fulfilled!:
        Assert.assertEquals(Status.PENDING, downloadSnapshotStep.getStatus());
    }
}
