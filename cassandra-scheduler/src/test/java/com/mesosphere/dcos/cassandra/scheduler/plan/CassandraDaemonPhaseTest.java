package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.scheduler.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.config.ConfigStoreException;
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
    private DefaultConfigurationManager configurationManager;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEmptyPhase() throws ConfigStoreException {
        CassandraSchedulerConfiguration configuration = Mockito.mock(CassandraSchedulerConfiguration.class);
        when(configurationManager.getTargetConfig()).thenReturn(configuration);
        Mockito.when(configuration.getServers()).thenReturn(0);
        final CassandraDaemonPhase phase = CassandraDaemonPhase.create(
                cassandraTasks,
                persistentOfferRequirementProvider,
                client,
                configurationManager);
        Assert.assertTrue(CollectionUtils.isEmpty(phase.getErrors()));
        Assert.assertTrue(phase.getBlocks().size() == 0);
        Assert.assertEquals("Deploy", phase.getName());
    }

    @Test
    public void testCreateSingleBlockPhase() throws Exception {
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        final String EXPECTED_NAME = "node-0";
        when(cassandraTasks.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        CassandraSchedulerConfiguration configuration = Mockito.mock(CassandraSchedulerConfiguration.class);
        when(configurationManager.getTargetConfig()).thenReturn(configuration);
        Mockito.when(configuration.getServers()).thenReturn(1);
        final CassandraDaemonPhase phase = CassandraDaemonPhase.create(
                cassandraTasks,
                persistentOfferRequirementProvider,
                client,
                configurationManager);
        Assert.assertTrue(CollectionUtils.isEmpty(phase.getErrors()));
        Assert.assertTrue(phase.getBlocks().size() == 1);
        Assert.assertEquals("Deploy", phase.getName());
    }
}
