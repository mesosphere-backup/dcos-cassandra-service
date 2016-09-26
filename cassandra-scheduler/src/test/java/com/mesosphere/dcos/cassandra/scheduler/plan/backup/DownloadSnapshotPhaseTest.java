package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;

public class DownloadSnapshotPhaseTest {
    @Mock
    private ClusterTaskOfferRequirementProvider provider;
    @Mock
    private CassandraState cassandraState;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBlocksEmpty() {
        final BackupRestoreContext context =  BackupRestoreContext.create("", "", "", "", "", "", false);

        when(cassandraState.getDaemons()).thenReturn(MapUtils.EMPTY_MAP);
        final DownloadSnapshotPhase phase = new DownloadSnapshotPhase(context, cassandraState, provider);
        final List<DownloadSnapshotBlock> blocks = phase.createBlocks();

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
        final List<DownloadSnapshotBlock> blocks = phase.createBlocks();

        Assert.assertNotNull(blocks);
        Assert.assertTrue(blocks.size() == 1);

        Assert.assertEquals("download-node-0", blocks.get(0).getName());
    }
}
