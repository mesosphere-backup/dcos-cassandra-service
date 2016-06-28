package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class CassandraDaemonPhaseTest {
    @Mock
    private PersistentOfferRequirementProvider persistentOfferRequirementProvider;
    @Mock
    private CassandraTasks cassandraTasks;
    @Mock
    private SchedulerClient client;
    @Mock
    private ConfigurationManager configurationManager;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEmptyPhase() {
        final CassandraDaemonPhase phase = CassandraDaemonPhase.create(
                configurationManager,
                cassandraTasks,
                persistentOfferRequirementProvider,
                client);
        Assert.assertTrue(CollectionUtils.isEmpty(phase.getErrors()));
        Assert.assertTrue(phase.getBlocks().size() == 0);
        Assert.assertEquals("Deploy", phase.getName());
    }

    @Test
    public void testCreateSingleBlockPhase() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        Mockito.when(configurationManager.getServers()).thenReturn(1);
        final CassandraDaemonPhase phase = CassandraDaemonPhase.create(
                configurationManager,
                cassandraTasks,
                persistentOfferRequirementProvider,
                client);
        Assert.assertTrue(CollectionUtils.isEmpty(phase.getErrors()));
        Assert.assertTrue(phase.getBlocks().size() == 1);
        Assert.assertEquals("Deploy", phase.getName());
    }
}
