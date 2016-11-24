package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.serialization.JsonSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestoreManagerTest {
    private static final String RESTORE_NODE_0 = "restore-node-0";
    private static final String DOWNLOAD_NODE_0 = "download-node-0";
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
        when(mockState.fetchProperty(RestoreManager.RESTORE_KEY)).thenThrow(
                new StateStoreException("no state found"));
        RestoreManager manager = new RestoreManager(mockCassandraState, mockProvider, mockState);
        assertFalse(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertTrue(manager.getPhases().isEmpty());
    }

    @Test
    public void testInitialWithState() throws SerializationException {
        final BackupRestoreContext context =  BackupRestoreContext.create("", "", "", "", "", "", false);
        when(mockState.fetchProperty(RestoreManager.RESTORE_KEY)).thenReturn(
                JsonSerializer.create(BackupRestoreContext.class).serialize(context));
        RestoreManager manager = new RestoreManager(mockCassandraState, mockProvider, mockState);
        assertTrue(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());
    }

    @Test
    public void testStartCompleteStop() throws PersistenceException {
        when(mockState.fetchProperty(RestoreManager.RESTORE_KEY)).thenThrow(
                new StateStoreException("no state found"));
        RestoreManager manager = new RestoreManager(mockCassandraState, mockProvider, mockState);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(mockCassandraState.getDaemons()).thenReturn(map);
        when(mockCassandraState.get(RESTORE_NODE_0)).thenReturn(Optional.of(daemonTask));
        when(mockCassandraState.get(DOWNLOAD_NODE_0)).thenReturn(Optional.of(daemonTask));

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
        when(mockState.fetchProperty(RestoreManager.RESTORE_KEY)).thenThrow(
                new StateStoreException("no state found"));
        RestoreManager manager = new RestoreManager(mockCassandraState, mockProvider, mockState);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(mockCassandraState.getDaemons()).thenReturn(map);
        when(mockCassandraState.get(RESTORE_NODE_0)).thenReturn(Optional.of(daemonTask));
        when(mockCassandraState.get(DOWNLOAD_NODE_0)).thenReturn(Optional.of(daemonTask));

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
        Map<String, DownloadSnapshotTask> previousDownloadTasks =
                new HashMap<String, DownloadSnapshotTask>();
        previousDownloadTasks.put("hey", DownloadSnapshotTask.parse(TaskInfo.getDefaultInstance()));
        Map<String, RestoreSnapshotTask> previousRestoreTasks =
                new HashMap<String, RestoreSnapshotTask>();
        previousRestoreTasks.put("hi", RestoreSnapshotTask.parse(TaskInfo.getDefaultInstance()));
        when(mockCassandraState.getDownloadSnapshotTasks()).thenReturn(previousDownloadTasks);
        when(mockCassandraState.getRestoreSnapshotTasks()).thenReturn(previousRestoreTasks);

        manager.start(emptyRequest());

        verify(mockCassandraState).remove("hi");
        verify(mockCassandraState).remove("hey");

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(2, manager.getPhases().size());
    }

    @Test
    public void testStartStop() {
        when(mockState.fetchProperty(RestoreManager.RESTORE_KEY)).thenThrow(
                new StateStoreException("no state found"));
        RestoreManager manager = new RestoreManager(mockCassandraState, mockProvider, mockState);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(mockCassandraState.getDaemons()).thenReturn(map);
        when(mockCassandraState.get(RESTORE_NODE_0)).thenReturn(Optional.of(daemonTask));
        when(mockCassandraState.get(DOWNLOAD_NODE_0)).thenReturn(Optional.of(daemonTask));

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
    public void testCreateBlocksEmpty() {
        final BackupRestoreContext context =  BackupRestoreContext.create("", "", "", "", "", "", false);

        when(cassandraState.getDaemons()).thenReturn(Collections.emptyMap());
        final DownloadSnapshotPhase phase = new DownloadSnapshotPhase(context, cassandraState, provider);
        final List<DownloadSnapshotStep> blocks = phase.createSteps();

        Assert.assertNotNull(blocks);
        Assert.assertTrue(CollectionUtils.isEmpty(blocks));
        Assert.assertEquals("Download", phase.getName());
    }

    @Test
    public void testCreateBlocksSingle() {
        final BackupRestoreContext context =  BackupRestoreContext.create("", "", "", "", "", "", false);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", daemonTask);
        when(cassandraState.getDaemons()).thenReturn(map);
        when(cassandraState.get("download-node-0")).thenReturn(Optional.of(daemonTask));
        final DownloadSnapshotPhase phase = new DownloadSnapshotPhase(context, cassandraState, provider);
        final List<DownloadSnapshotStep> blocks = phase.createSteps();

        Assert.assertNotNull(blocks);
        Assert.assertTrue(blocks.size() == 1);

        Assert.assertEquals("download-node-0", blocks.get(0).getName());
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
