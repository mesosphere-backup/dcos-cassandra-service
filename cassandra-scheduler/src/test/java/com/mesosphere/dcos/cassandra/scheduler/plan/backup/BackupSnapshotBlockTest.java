package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
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

public class BackupSnapshotBlockTest {
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
        Mockito.when(cassandraTasks.get("snapshot-node-0")).thenReturn(Optional.empty());
        final BackupContext backupContext = BackupContext.create("", "", "", "", "", "");
        final BackupSnapshotBlock backupSnapshotBlock = BackupSnapshotBlock.create(
                "node-0",
                cassandraTasks,
                provider,
                backupContext);
        Assert.assertEquals("snapshot-node-0", backupSnapshotBlock.getName());
        Assert.assertEquals("node-0", backupSnapshotBlock.getDaemon());
        Assert.assertEquals(Status.Pending, backupSnapshotBlock.getStatus());
    }

    @Test
    public void testComplete() {
        final CassandraTask mockCassandraTask = Mockito.mock(CassandraTask.class);
        Mockito.when(mockCassandraTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        Mockito.when(cassandraTasks.get("snapshot-node-0"))
                .thenReturn(Optional.ofNullable(mockCassandraTask));
        final BackupContext backupContext = BackupContext.create("", "", "", "", "", "");
        final BackupSnapshotBlock backupSnapshotBlock = BackupSnapshotBlock.create(
                "node-0",
                cassandraTasks,
                provider,
                backupContext);
        Assert.assertEquals("snapshot-node-0", backupSnapshotBlock.getName());
        Assert.assertEquals("node-0", backupSnapshotBlock.getDaemon());
        Assert.assertEquals(Status.Complete, backupSnapshotBlock.getStatus());
    }

    @Test
    public void testTaskStartAlreadyCompleted() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get("snapshot-node-0")).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", null);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupContext backupContext = BackupContext.create("", "", "", "", "", "");

        final BackupSnapshotTask snapshotTask = Mockito.mock(BackupSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito
                .when(cassandraTasks.getOrCreateBackupSnapshot(daemonTask, backupContext))
                .thenReturn(snapshotTask);

        final BackupSnapshotBlock backupSnapshotBlock = BackupSnapshotBlock.create(
                "node-0",
                cassandraTasks,
                provider,
                backupContext);

        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any())).thenReturn(requirement);
        Assert.assertNull(backupSnapshotBlock.start());
        Assert.assertEquals(Status.Complete, backupSnapshotBlock.getStatus());
    }

    @Test
    public void testTaskStart() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get("snapshot-node-0")).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", daemonTask);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupContext backupContext = BackupContext.create("", "", "", "", "", "");

        final BackupSnapshotTask snapshotTask = Mockito.mock(BackupSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito
                .when(cassandraTasks.getOrCreateBackupSnapshot(daemonTask, backupContext))
                .thenReturn(snapshotTask);

        final BackupSnapshotBlock backupSnapshotBlock = BackupSnapshotBlock.create(
                "node-0",
                cassandraTasks,
                provider,
                backupContext);

        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any())).thenReturn(requirement);
        Assert.assertNotNull(backupSnapshotBlock.start());
        Assert.assertEquals(Status.InProgress, backupSnapshotBlock.getStatus());
    }

    @Test
    public void testTaskFailed() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraTasks.get("snapshot-node-0")).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", daemonTask);
        Mockito.when(cassandraTasks.getDaemons()).thenReturn(map);
        final BackupContext backupContext = BackupContext.create("", "", "", "", "", "");

        final BackupSnapshotTask snapshotTask = Mockito.mock(BackupSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito.when(cassandraTasks.getOrCreateBackupSnapshot(daemonTask, backupContext))
                .thenThrow(PersistenceException.class);

        final BackupSnapshotBlock backupSnapshotBlock = BackupSnapshotBlock.create(
                "node-0",
                cassandraTasks,
                provider,
                backupContext);
        Assert.assertNull(backupSnapshotBlock.start());
    }
}
