package com.mesosphere.dcos.cassandra.scheduler;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.config.MesosConfig;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPhaseStrategies;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraStageManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.StageManager;
import org.apache.mesos.state.StateStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CassandraSchedulerTest {
    private CassandraScheduler scheduler;
    private IdentityManager identityManager;
    private ConfigurationManager configurationManager;
    private StageManager stageManager;
    private PersistentOfferRequirementProvider offerRequirementProvider;
    private CassandraTasks cassandraTasks;
    private Reconciler reconciler;
    private EventBus eventBus;
    private SchedulerClient client;
    private BackupManager backup;
    private RestoreManager restore;
    private CleanupManager cleanup;
    private RepairManager repair;
    private SeedsManager seeds;
    private ExecutorService executorService;
    private MesosConfig mesosConfig;
    private SchedulerDriver mockSchedulerDriver;
    private Protos.FrameworkID frameworkId;
    private Protos.MasterInfo masterInfo;
    private StateStore stateStore;
    private static String testDaemonName = "test-daemon-name";

    @Before
    public void before() throws Exception {
        mesosConfig = Mockito.mock(MesosConfig.class);
        identityManager = Mockito.mock(IdentityManager.class);
        configurationManager = Mockito.mock(ConfigurationManager.class);
        offerRequirementProvider = Mockito.mock(PersistentOfferRequirementProvider.class);
        cassandraTasks = Mockito.mock(CassandraTasks.class);
        reconciler = Mockito.mock(Reconciler.class);
        eventBus = new EventBus();
        client = Mockito.mock(SchedulerClient.class);
        backup = Mockito.mock(BackupManager.class);
        restore = Mockito.mock(RestoreManager.class);
        cleanup = Mockito.mock(CleanupManager.class);
        repair = Mockito.mock(RepairManager.class);
        seeds = Mockito.mock(SeedsManager.class);
        executorService = Executors.newCachedThreadPool();
        mockSchedulerDriver = Mockito.mock(SchedulerDriver.class);
        frameworkId = TestUtils.generateFrameworkId();
        stateStore = Mockito.mock(StateStore.class);

        Mockito.when(configurationManager.getServers()).thenReturn(3);
        Mockito.when(cassandraTasks.getStateStore()).thenReturn(stateStore);

        stageManager = new CassandraStageManager(
                new CassandraPhaseStrategies("org.apache.mesos.scheduler.plan.DefaultInstallStrategy"));

        final CassandraContainer node0 = Mockito.mock(CassandraContainer.class);
        Mockito.when(cassandraTasks.getOrCreateContainer("node-0")).thenReturn(node0);
        final CassandraContainer node1 = Mockito.mock(CassandraContainer.class);
        Mockito.when(cassandraTasks.getOrCreateContainer("node-1")).thenReturn(node1);
        final CassandraContainer node2 = Mockito.mock(CassandraContainer.class);
        Mockito.when(cassandraTasks.getOrCreateContainer("node-2")).thenReturn(node2);

        scheduler = new CassandraScheduler(
                configurationManager,
                identityManager,
                mesosConfig,
                offerRequirementProvider,
                stageManager,
                cassandraTasks,
                reconciler,
                client,
                eventBus,
                backup,
                restore,
                cleanup,
                repair,
                seeds,
                executorService);

        masterInfo = TestUtils.generateMasterInfo();
        scheduler.registered(null, frameworkId, masterInfo);

        Mockito.when(identityManager.isRegistered()).thenReturn(true);
    }

    @Test
    public void testRegistered() throws Exception {
        Mockito.verify(identityManager).register(frameworkId.getValue());
    }

    @Test
    public void testResourceOffersEmpty() throws Exception {
        scheduler.resourceOffers(mockSchedulerDriver, Collections.emptyList());

        Mockito.verify(reconciler).reconcile(mockSchedulerDriver);
        Mockito.verify(identityManager, Mockito.times(1)).isRegistered();
        Mockito.verify(mockSchedulerDriver, Mockito.never()).acceptOffers(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testResourceOffersOneInsufficientOfferCycle() throws Exception {
        // Insufficient offers
        scheduler.resourceOffers(mockSchedulerDriver,
                Arrays.asList(TestUtils.generateOffer(
                        frameworkId.getValue(),
                        1,
                        1024,
                        1024)));


        Mockito.verify(mockSchedulerDriver, Mockito.never()).acceptOffers(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockSchedulerDriver).declineOffer(Mockito.any());
    }
}
