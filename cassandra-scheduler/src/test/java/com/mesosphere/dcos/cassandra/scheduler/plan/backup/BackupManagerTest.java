package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.scheduler.resources.BackupRestoreRequest;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.config.SerializationUtils;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupManagerTest {
    private static final String SNAPSHOT_NODE_0 = "snapshot-node-0";
    private static final String UPLOAD_NODE_0 = "upload-node-0";
    private static final String BACKUPSCHEMA_NODE_0 = "backupschema-node-0";
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
    public void testInitialWithState() throws IOException {
        final BackupRestoreContext context =  BackupRestoreContext.create("", "", "", "", "", "", false, "","","");
        when(mockState.fetchProperty(BackupManager.BACKUP_KEY)).thenReturn(
                SerializationUtils.toJsonString(context).getBytes(StandardCharsets.UTF_8));
        BackupManager manager = new BackupManager(mockCassandraState, mockProvider, mockState);
        assertTrue(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertEquals(3, manager.getPhases().size());
    }

    @Test
    public void testStartCompleteStop() {
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
        when(mockCassandraState.get(BACKUPSCHEMA_NODE_0)).thenReturn(Optional.of(daemonTask));

        manager.start(emptyRequest());

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(3, manager.getPhases().size());

        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        // notify steps to check for TASK_FINISHED:
        for (Phase phase : manager.getPhases()) {
            for (Step step : phase.getChildren()) {
                step.update(TaskStatus.getDefaultInstance());
            }
        }

        assertTrue(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertEquals(3, manager.getPhases().size());

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
        when(mockCassandraState.get(BACKUPSCHEMA_NODE_0)).thenReturn(Optional.of(daemonTask));

        manager.start(emptyRequest());

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(3, manager.getPhases().size());

        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_FINISHED);
        // notify steps to check for TASK_FINISHED:
        for (Phase phase : manager.getPhases()) {
            for (Step step : phase.getChildren()) {
                step.update(TaskStatus.getDefaultInstance());
            }
        }

        assertTrue(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertEquals(3, manager.getPhases().size());

        Mockito.when(daemonTask.getState()).thenReturn(Protos.TaskState.TASK_RUNNING);
        Map<String, BackupUploadTask> previousUploadTasks = new HashMap<String, BackupUploadTask>();
        previousUploadTasks.put("hey", BackupUploadTask.parse(TaskInfo.getDefaultInstance()));
        Map<String, BackupSnapshotTask> previousBackupTasks = new HashMap<String, BackupSnapshotTask>();
        previousBackupTasks.put("hi", BackupSnapshotTask.parse(TaskInfo.getDefaultInstance()));
        Map<String, BackupSchemaTask> previousSchemaTasks = new HashMap<>();
        previousSchemaTasks.put("hello", BackupSchemaTask.parse(TaskInfo.getDefaultInstance()));
        when(mockCassandraState.getBackupUploadTasks()).thenReturn(previousUploadTasks);
        when(mockCassandraState.getBackupSnapshotTasks()).thenReturn(previousBackupTasks);
        when(mockCassandraState.getBackupSchemaTasks()).thenReturn(previousSchemaTasks);

        manager.start(emptyRequest());

        verify(mockCassandraState).remove(Collections.singleton("hi"));
        verify(mockCassandraState).remove(Collections.singleton("hey"));
        verify(mockCassandraState).remove(Collections.singleton("hello"));

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(3, manager.getPhases().size());
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
        when(mockCassandraState.get(BACKUPSCHEMA_NODE_0)).thenReturn(Optional.of(daemonTask));

        manager.start(emptyRequest());

        assertFalse(manager.isComplete());
        assertTrue(manager.isInProgress());
        assertEquals(3, manager.getPhases().size());

        manager.stop();

        assertFalse(manager.isComplete());
        assertFalse(manager.isInProgress());
        assertTrue(manager.getPhases().isEmpty());
    }

    private BackupRestoreRequest emptyRequest() {
        BackupRestoreRequest request = new BackupRestoreRequest();
        request.setAzureAccount("");
        request.setAzureKey("");
        request.setExternalLocation("");
        request.setName("");
        request.setS3AccessKey("");
        request.setS3SecretKey("");
        request.setRestoreType("");
        return request;
    }
}
