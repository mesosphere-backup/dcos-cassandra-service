package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
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

public class BackupSnapshotPhaseTest {
    @Mock
    private ClusterTaskOfferRequirementProvider provider;
    @Mock
    private CassandraTasks cassandraTasks;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBlocksEmpty() {
        final BackupContext context =  BackupContext.create("", "", "", "", "", "");

        when(cassandraTasks.getDaemons()).thenReturn(MapUtils.EMPTY_MAP);
        final BackupSnapshotPhase phase = new BackupSnapshotPhase(context, cassandraTasks, provider);
        final List<BackupSnapshotBlock> blocks = phase.createBlocks();

        Assert.assertNotNull(blocks);
        Assert.assertTrue(CollectionUtils.isEmpty(blocks));
    }

    @Test
    public void testCreateBlocksSingle() {
        final BackupContext context =  BackupContext.create("", "", "", "", "", "");

        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        final HashMap<String, CassandraDaemonTask> map = new HashMap<>();
        map.put("node-0", daemonTask);
        when(cassandraTasks.getDaemons()).thenReturn(map);
        when(cassandraTasks.get("snapshot-node-0")).thenReturn(Optional.of(daemonTask));
        final BackupSnapshotPhase phase = new BackupSnapshotPhase(context, cassandraTasks, provider);
        final List<BackupSnapshotBlock> blocks = phase.createBlocks();

        Assert.assertNotNull(blocks);
        Assert.assertTrue(blocks.size() == 1);
    }
}
