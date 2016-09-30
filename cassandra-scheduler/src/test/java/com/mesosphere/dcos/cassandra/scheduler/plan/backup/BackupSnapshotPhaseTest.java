package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.mesos.scheduler.plan.Block;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.when;

public class BackupSnapshotPhaseTest {
    public static final String SNAPSHOT_NODE_0 = "snapshot-node-0";
    public static final String NODE_0 = "node-0";
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
        final BackupSnapshotPhase phase = new BackupSnapshotPhase(context, cassandraState, provider);
        final List<BackupSnapshotBlock> blocks = phase.createBlocks();

        Assert.assertNotNull(blocks);
        Assert.assertTrue(CollectionUtils.isEmpty(blocks));
        Assert.assertEquals("Snapshot", phase.getName());
    }

    @Test
    public void testCreateBlocksSingle() {
        final BackupRestoreContext context =  BackupRestoreContext.create("", "", "", "", "", "", false);

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put(NODE_0, daemonTask);
        when(cassandraState.getDaemons()).thenReturn(map);
        when(cassandraState.get(SNAPSHOT_NODE_0)).thenReturn(Optional.of(daemonTask));
        final BackupSnapshotPhase phase = new BackupSnapshotPhase(context, cassandraState, provider);
        final List<? extends Block> blocks = phase.getBlocks();

        Assert.assertNotNull(blocks);
        Assert.assertTrue(blocks.size() == 1);

        final Block block = blocks.get(0);
        Assert.assertTrue(block instanceof BackupSnapshotBlock);
        Assert.assertEquals("snapshot-node-0", block.getName());

        final UUID blockId = block.getId();

        final UUID getId = UUID.fromString(blockId.toString());
        Assert.assertEquals(block, phase.getBlock(getId));
    }
}
