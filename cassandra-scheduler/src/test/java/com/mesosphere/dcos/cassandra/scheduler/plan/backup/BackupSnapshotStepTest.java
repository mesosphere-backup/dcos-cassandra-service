package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
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

public class BackupSnapshotStepTest {
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
        Mockito.when(cassandraState.get("snapshot-node-0")).thenReturn(Optional.empty());
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");
        final BackupSnapshotStep backupSnapshotStep =
                new BackupSnapshotStep("node-0", cassandraState, provider, backupRestoreContext);
        Assert.assertEquals("snapshot-node-0", backupSnapshotStep.getName());
        Assert.assertEquals("node-0", backupSnapshotStep.getDaemon());
        Assert.assertEquals(Status.PENDING, backupSnapshotStep.getStatus());
    }

    @Test
    public void testComplete() {
        final CassandraTask mockCassandraTask = Mockito.mock(CassandraTask.class);
        Mockito.when(mockCassandraTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        Mockito.when(cassandraState.get("snapshot-node-0"))
                .thenReturn(Optional.ofNullable(mockCassandraTask));
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");
        final BackupSnapshotStep backupSnapshotStep =
                new BackupSnapshotStep("node-0", cassandraState, provider, backupRestoreContext);
        Assert.assertEquals("snapshot-node-0", backupSnapshotStep.getName());
        Assert.assertEquals("node-0", backupSnapshotStep.getDaemon());
        Assert.assertEquals(Status.COMPLETE, backupSnapshotStep.getStatus());
    }

    @Test
    public void testTaskStartAlreadyCompleted() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraState.get("snapshot-node-0")).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", null);
        Mockito.when(cassandraState.getDaemons()).thenReturn(map);
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");

        final BackupSnapshotTask snapshotTask = Mockito.mock(BackupSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito.when(cassandraState.getOrCreateBackupSnapshot(daemonTask, backupRestoreContext))
                .thenReturn(snapshotTask);

        final BackupSnapshotStep backupSnapshotStep =
                new BackupSnapshotStep("node-0", cassandraState, provider, backupRestoreContext);

        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any(), Mockito.any())).thenReturn(requirement);
        Assert.assertTrue(!backupSnapshotStep.start().isPresent());
        Assert.assertEquals(Status.COMPLETE, backupSnapshotStep.getStatus());
    }

    @Test
    public void testTaskStart() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraState.get("snapshot-node-0")).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", daemonTask);
        Mockito.when(cassandraState.getDaemons()).thenReturn(map);
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");

        final BackupSnapshotTask snapshotTask = Mockito.mock(BackupSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito.when(snapshotTask.getType()).thenReturn(CassandraTask.TYPE.BACKUP_SNAPSHOT);
        Mockito.when(cassandraState.getOrCreateBackupSnapshot(daemonTask, backupRestoreContext))
                .thenReturn(snapshotTask);

        final BackupSnapshotStep backupSnapshotStep =
                new BackupSnapshotStep("node-0", cassandraState, provider, backupRestoreContext);
        final OfferRequirement requirement = Mockito.mock(OfferRequirement.class);
        Mockito.when(provider.getUpdateOfferRequirement(Mockito.any(), Mockito.any())).thenReturn(requirement);
        Assert.assertNotNull(backupSnapshotStep.start());
        // not IN_PROGRESS until the requirement is fulfilled!:
        Assert.assertEquals(Status.PENDING, backupSnapshotStep.getStatus());
    }

    @Test
    public void testTaskFailed() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(cassandraState.get("snapshot-node-0")).thenReturn(Optional.empty());
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", daemonTask);
        Mockito.when(cassandraState.getDaemons()).thenReturn(map);
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("", "", "", "", "", "", false, "","","");

        final BackupSnapshotTask snapshotTask = Mockito.mock(BackupSnapshotTask.class);
        Mockito.when(snapshotTask.getSlaveId()).thenReturn("1234");
        Mockito.when(cassandraState.getOrCreateBackupSnapshot(daemonTask, backupRestoreContext))
                .thenThrow(PersistenceException.class);

        final BackupSnapshotStep backupSnapshotStep =
                new BackupSnapshotStep("node-0", cassandraState, provider, backupRestoreContext);
        Assert.assertTrue(!backupSnapshotStep.start().isPresent());
    }
}
