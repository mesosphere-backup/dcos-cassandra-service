package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.serialization.JsonSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.resources.BackupRestoreRequest;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupManagerTest {
    private static final String SNAPSHOT_NODE_0 = "snapshot-node-0";
    private static final String UPLOAD_NODE_0 = "upload-node-0";
    private static final String NODE_0 = "node-0";

    @Mock private ClusterTaskOfferRequirementProvider mockProvider;
    @Mock private CassandraState mockCassandraState;
    @Mock private StateStore mockState;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testInitialNoState() {
        when(mockState.fetchProperty(BackupManager.BACKUP_KEY)).thenThrow(
                new StateStoreException("no state found"));
        BackupManager manager = new BackupManager(mockCassandraState, mockProvider, mockState);
        assertFalse(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertTrue(manager.getPhases().isEmpty());
    }

    @Test
    public void testInitialWithState() throws SerializationException {
        final BackupRestoreContext context =  BackupRestoreContext.create("", "", "", "", "", "", false);
        when(mockState.fetchProperty(BackupManager.BACKUP_KEY)).thenReturn(
                JsonSerializer.create(BackupRestoreContext.class).serialize(context));
        BackupManager manager = new BackupManager(mockCassandraState, mockProvider, mockState);
        assertTrue(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());
    }

    @Test
    public void testStartCompleteStop() throws PersistenceException {
        when(mockState.fetchProperty(BackupManager.BACKUP_KEY)).thenThrow(
                new StateStoreException("no state found"));
        BackupManager manager = new BackupManager(mockCassandraState, mockProvider, mockState);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(mockCassandraState.getDaemons()).thenReturn(map);
        when(mockCassandraState.get(SNAPSHOT_NODE_0)).thenReturn(Optional.of(daemonTask));
        when(mockCassandraState.get(UPLOAD_NODE_0)).thenReturn(Optional.of(daemonTask));

        manager.start(emptyRequest());

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());

        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        // notify blocks to check for TASK_FINISHED:
        for (Phase phase : manager.getPhases()) {
            for (Step step : phase.getChildren()) {
                step.update(TaskStatus.getDefaultInstance());
            }
        }

        assertTrue(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());

        manager.stop();

        assertFalse(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertTrue(manager.getPhases().isEmpty());
    }

    @Test
    public void testStartCompleteStart() throws PersistenceException {
        when(mockState.fetchProperty(BackupManager.BACKUP_KEY)).thenThrow(
                new StateStoreException("no state found"));
        BackupManager manager = new BackupManager(mockCassandraState, mockProvider, mockState);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(mockCassandraState.getDaemons()).thenReturn(map);
        when(mockCassandraState.get(SNAPSHOT_NODE_0)).thenReturn(Optional.of(daemonTask));
        when(mockCassandraState.get(UPLOAD_NODE_0)).thenReturn(Optional.of(daemonTask));

        manager.start(emptyRequest());

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());

        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        // notify blocks to check for TASK_FINISHED:
        for (Phase phase : manager.getPhases()) {
            for (Step step : phase.getChildren()) {
                step.update(TaskStatus.getDefaultInstance());
            }
        }

        assertTrue(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());

        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        Map<String, BackupUploadTask> previousUploadTasks = new HashMap<String, BackupUploadTask>();
        previousUploadTasks.put("hey", BackupUploadTask.parse(TaskInfo.getDefaultInstance()));
        Map<String, BackupSnapshotTask> previousBackupTasks =
                new HashMap<String, BackupSnapshotTask>();
        previousBackupTasks.put("hi", BackupSnapshotTask.parse(TaskInfo.getDefaultInstance()));
        when(mockCassandraState.getBackupUploadTasks()).thenReturn(previousUploadTasks);
        when(mockCassandraState.getBackupSnapshotTasks()).thenReturn(previousBackupTasks);

        manager.start(emptyRequest());

        verify(mockCassandraState).remove("hi");
        verify(mockCassandraState).remove("hey");

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());
    }

    @Test
    public void testStartStop() {
        when(mockState.fetchProperty(BackupManager.BACKUP_KEY)).thenThrow(
                new StateStoreException("no state found"));
        BackupManager manager = new BackupManager(mockCassandraState, mockProvider, mockState);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(mockCassandraState.getDaemons()).thenReturn(map);
        when(mockCassandraState.get(SNAPSHOT_NODE_0)).thenReturn(Optional.of(daemonTask));
        when(mockCassandraState.get(UPLOAD_NODE_0)).thenReturn(Optional.of(daemonTask));

        manager.start(emptyRequest());

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());

        manager.stop();

        assertFalse(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertTrue(manager.getPhases().isEmpty());
    }

    @Test
    public void testCreateStepsEmpty() {
        final BackupRestoreContext context = BackupRestoreContext.create("", "", "", "", "", "", false);

        when(cassandraState.getDaemons()).thenReturn(Collections.emptyMap());
        final BackupSnapshotPhase phase = new BackupSnapshotPhase(context, cassandraState, provider);
        final List<BackupSnapshotStep> steps = phase.createSteps();

        Assert.assertNotNull(steps);
        Assert.assertTrue(CollectionUtils.isEmpty(steps));
        Assert.assertEquals("Snapshot", phase.getName());
    }

    @Test
    public void testCreateStepsSingle() {
        final BackupRestoreContext context = BackupRestoreContext.create("", "", "", "", "", "", false);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(cassandraState.getDaemons()).thenReturn(map);
        when(cassandraState.get(SNAPSHOT_NODE_0)).thenReturn(Optional.of(daemonTask));
        final BackupSnapshotPhase phase = new BackupSnapshotPhase(context, cassandraState, provider);
        final List<? extends Step> steps = phase.getBlocks();

        Assert.assertNotNull(steps);
        Assert.assertTrue(steps.size() == 1);

        final Step step = steps.get(0);
        Assert.assertTrue(step instanceof BackupSnapshotStep);
        Assert.assertEquals("snapshot-node-0", step.getName());

        final UUID blockId = step.getId();

        final UUID getId = UUID.fromString(blockId.toString());
        Assert.assertEquals(step, phase.getBlock(getId));
    }

    private BackupRestoreRequest emptyRequest() {
        BackupRestoreRequest request = new BackupRestoreRequest();
        request.setAzureAccount("");
        request.setAzureKey("");
        request.setExternalLocation("");
        request.setName("");
        request.setS3AccessKey("");
        request.setS3SecretKey("");
        return request;
    }
}
