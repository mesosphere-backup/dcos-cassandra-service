package com.mesosphere.dcos.cassandra.scheduler;

import com.google.inject.Injector;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import io.dropwizard.testing.ResourceHelpers;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.scheduler.plan.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class CassandraSchedulerTest {
    @ClassRule
    public static CassandraDropwizardAppRule<CassandraSchedulerConfiguration> RULE =
            new CassandraDropwizardAppRule<>(Main.class, ResourceHelpers.resourceFilePath("scheduler.yml"));

    @Test
    public void testRegistered() throws Exception {
        final Main main = (Main) RULE.getApplication();
        final Injector injector = main.getInjector();
        final CassandraScheduler scheduler = injector.getInstance(CassandraScheduler.class);
        final Protos.FrameworkID expectedFrameworkId = TestUtils.generateFrameworkId();
        scheduler.registered(null, expectedFrameworkId, TestUtils.generateMasterInfo());
        final IdentityManager identityManager = injector.getInstance(IdentityManager.class);
        final String actualFrameworkId = identityManager.get().getId();
        final StageManager stageManager = injector.getInstance(StageManager.class);

        Assert.assertEquals(expectedFrameworkId.getValue(), actualFrameworkId);
        final Stage stage = stageManager.getStage();
        Assert.assertNotNull(stage);
        Assert.assertEquals(3, stage.getPhases().size());
        final Phase reconcilePhase = stage.getPhases().get(0);
        Assert.assertEquals(1, reconcilePhase.getBlocks().size());
        final Phase syncData = stage.getPhases().get(1);
        Assert.assertEquals(0, syncData.getBlocks().size());
        final Phase nodePhase = stage.getPhases().get(2);
        Assert.assertEquals(3, nodePhase.getBlocks().size());
    }

    @Test
    public void testResourceOffersEmpty() throws Exception {
        final Main main = (Main) RULE.getApplication();
        final Injector injector = main.getInjector();
        final CassandraScheduler scheduler = injector.getInstance(CassandraScheduler.class);
        final SchedulerDriver mockSchedulerDriver = Mockito.mock(SchedulerDriver.class);
        Mockito.when(mockSchedulerDriver.acceptOffers(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);
        Mockito.when(mockSchedulerDriver.declineOffer(Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);
        Mockito.when(mockSchedulerDriver.declineOffer(Mockito.any(), Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);

        scheduler.registered(null, TestUtils.generateFrameworkId(), TestUtils.generateMasterInfo());
        scheduler.resourceOffers(mockSchedulerDriver, Collections.emptyList());

        final StageManager stageManager = injector.getInstance(StageManager.class);

        Assert.assertEquals("Reconciliation", stageManager.getCurrentPhase().getName());
        Assert.assertEquals("Reconciliation", stageManager.getCurrentBlock().getName());
        Assert.assertEquals(Status.InProgress, stageManager.getCurrentBlock().getStatus());
    }

    @Test
    public void testResourceOffersOneInsufficientOfferCycle() throws Exception {
        final Main main = (Main) RULE.getApplication();
        final Injector injector = main.getInjector();
        final CassandraScheduler scheduler = injector.getInstance(CassandraScheduler.class);
        final SchedulerDriver mockSchedulerDriver = Mockito.mock(SchedulerDriver.class);
        Mockito.when(mockSchedulerDriver.acceptOffers(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);
        Mockito.when(mockSchedulerDriver.declineOffer(Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);
        Mockito.when(mockSchedulerDriver.declineOffer(Mockito.any(), Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);

        final Protos.FrameworkID frameworkId = TestUtils.generateFrameworkId();
        scheduler.registered(null, frameworkId, TestUtils.generateMasterInfo());

        final StageManager stageManager = injector.getInstance(StageManager.class);
        final Phase reconcilePhase = stageManager.getCurrentPhase();
        final Block reconcileBlock = stageManager.getCurrentBlock();
        stageManager.forceComplete(reconcilePhase.getId(), reconcileBlock.getId());

        // Insufficient offers
        scheduler.resourceOffers(mockSchedulerDriver,
                Arrays.asList(TestUtils.generateOffer(
                        frameworkId.getValue(),
                        1,
                        1024,
                        1024)));


        Assert.assertEquals("Deploy", stageManager.getCurrentPhase().getName());
        Assert.assertEquals("node-0", stageManager.getCurrentBlock().getName());
        Assert.assertEquals(Status.Pending, stageManager.getCurrentBlock().getStatus());
    }

    @Test
    public void testResourceOffersThreeSufficientOffersCycle() throws Exception {
        final Main main = (Main) RULE.getApplication();
        final Injector injector = main.getInjector();
        final CassandraScheduler scheduler = injector.getInstance(CassandraScheduler.class);
        final SchedulerDriver mockSchedulerDriver = Mockito.mock(SchedulerDriver.class);
        Mockito.when(mockSchedulerDriver.acceptOffers(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);
        Mockito.when(mockSchedulerDriver.declineOffer(Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);
        Mockito.when(mockSchedulerDriver.declineOffer(Mockito.any(), Mockito.any())).thenReturn(Protos.Status.DRIVER_RUNNING);

        final Protos.FrameworkID frameworkId = TestUtils.generateFrameworkId();
        scheduler.registered(null, frameworkId, TestUtils.generateMasterInfo());

        final StageManager stageManager = injector.getInstance(StageManager.class);
        final Phase reconcilePhase = stageManager.getCurrentPhase();
        final Block reconcileBlock = stageManager.getCurrentBlock();

        Assert.assertEquals("Reconciliation", reconcilePhase.getName());
        Assert.assertEquals("Reconciliation", reconcileBlock.getName());

        stageManager.forceComplete(reconcilePhase.getId(), reconcileBlock.getId());

        scheduler.resourceOffers(mockSchedulerDriver,
                Arrays.asList(TestUtils.generateOffer(
                        frameworkId.getValue(),
                        8,
                        10240,
                        20480)));

        Assert.assertEquals("Deploy", stageManager.getCurrentPhase().getName());
        Assert.assertEquals("node-0", stageManager.getCurrentBlock().getName());
        Assert.assertEquals(Status.InProgress, stageManager.getCurrentBlock().getStatus());

        final CassandraTasks cassandraTasks = injector.getInstance(CassandraTasks.class);
        Map<String, CassandraDaemonTask> daemons = cassandraTasks.getDaemons();
        final CassandraDaemonTask task0 = daemons.get("node-0");
        Assert.assertNotNull(task0);
        final Protos.TaskID taskId = task0.getTaskInfo().getTaskId();
        final Protos.TaskStatus status = TestUtils.generateStatus(taskId, Protos.TaskState.TASK_RUNNING);
        scheduler.statusUpdate(mockSchedulerDriver, status);

        Assert.assertEquals("Deploy", stageManager.getCurrentPhase().getName());
        Assert.assertEquals("node-1", stageManager.getCurrentBlock().getName());
        Assert.assertEquals(Status.Pending, stageManager.getCurrentBlock().getStatus());

        scheduler.resourceOffers(mockSchedulerDriver,
                Arrays.asList(TestUtils.generateOffer(
                        frameworkId.getValue(),
                        8,
                        10240,
                        20480)));

        Assert.assertEquals("Deploy", stageManager.getCurrentPhase().getName());
        Assert.assertEquals("node-1", stageManager.getCurrentBlock().getName());
        Assert.assertEquals(Status.InProgress, stageManager.getCurrentBlock().getStatus());

        daemons = cassandraTasks.getDaemons();
        final CassandraDaemonTask task1 = daemons.get("node-1");
        Assert.assertNotNull(task1);
        final Protos.TaskID taskId1 = task1.getTaskInfo().getTaskId();
        final Protos.TaskStatus status1 = TestUtils.generateStatus(taskId1, Protos.TaskState.TASK_RUNNING);
        scheduler.statusUpdate(mockSchedulerDriver, status1);

        Assert.assertEquals("Deploy", stageManager.getCurrentPhase().getName());
        Assert.assertEquals("node-2", stageManager.getCurrentBlock().getName());
        Assert.assertEquals(Status.Pending, stageManager.getCurrentBlock().getStatus());

        scheduler.resourceOffers(mockSchedulerDriver,
                Arrays.asList(TestUtils.generateOffer(
                        frameworkId.getValue(),
                        8,
                        10240,
                        20480)));

        Assert.assertEquals("Deploy", stageManager.getCurrentPhase().getName());
        Assert.assertEquals("node-2", stageManager.getCurrentBlock().getName());
        Assert.assertEquals(Status.InProgress, stageManager.getCurrentBlock().getStatus());

        daemons = cassandraTasks.getDaemons();
        final CassandraDaemonTask task2 = daemons.get("node-2");
        Assert.assertNotNull(task2);
        final Protos.TaskID taskId2 = task2.getTaskInfo().getTaskId();
        final Protos.TaskStatus status2 = TestUtils.generateStatus(taskId2, Protos.TaskState.TASK_RUNNING);
        scheduler.statusUpdate(mockSchedulerDriver, status2);

        Assert.assertNull(stageManager.getCurrentPhase());
        Assert.assertNull(stageManager.getCurrentBlock());
    }
}
