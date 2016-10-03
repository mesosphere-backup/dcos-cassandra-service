package com.mesosphere.dcos.cassandra.scheduler.tasks;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.state.StateStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import static org.mockito.Mockito.when;

/**
 * This class tests the CassandraState class.
 */
public class CassandraStateTest {
    private static TestingServer server;
    private static MutableSchedulerConfiguration config;
    private static IdentityManager identity;
    private static ConfigurationManager configuration;
    private static ClusterTaskConfig clusterTaskConfig;
    private static String testDaemonName = "test-daemon-name";
    private static String testHostName = "test-host-name";
    private static String testTaskId = "test-task-id__1234";
    private CassandraState cassandraState;
    private static StateStore stateStore;

    @Before
    public void beforeEach() throws Exception {
        server = new TestingServer();

        server.start();

        final ConfigurationFactory<MutableSchedulerConfiguration> factory =
                new ConfigurationFactory<>(
                        MutableSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(
                                new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");

        config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());

        ServiceConfig initial = config.createConfig().getServiceConfig();

        final CassandraSchedulerConfiguration targetConfig = config.createConfig();
        clusterTaskConfig = targetConfig.getClusterTaskConfig();

        final CuratorFrameworkConfig curatorConfig = config.getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        stateStore = new CuratorStateStore(
                targetConfig.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        stateStore.storeFrameworkId(Protos.FrameworkID.newBuilder().setValue("1234").build());
        identity = new IdentityManager(
                initial,stateStore);

        identity.register("test_id");

        DefaultConfigurationManager configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                config.createConfig().getServiceConfig().getName(),
                server.getConnectString(),
                config.createConfig(),
                new ConfigValidator(),
                stateStore);

        Capabilities mockCapabilities = Mockito.mock(Capabilities.class);
        when(mockCapabilities.supportsNamedVips()).thenReturn(true);
        configuration = new ConfigurationManager(
                new CassandraDaemonTask.Factory(mockCapabilities),
                configurationManager);

        cassandraState = new CassandraState(
                configuration,
                clusterTaskConfig,
                stateStore);
    }

    @After
    public void afterEach() throws Exception {
        server.close();
        server.stop();
    }

    @Test
    public void testCreateCassandraContainer() throws Exception {
        CassandraContainer container = getTestCassandraContainer();
        Collection<Protos.TaskInfo> taskInfos = container.getTaskInfos();
        Assert.assertEquals(2, taskInfos.size());

        Iterator<Protos.TaskInfo> iter = taskInfos.iterator();
        Protos.TaskInfo daemonTaskInfo = iter.next();
        Protos.TaskInfo clusterTemplateTaskInfo = iter.next();

        validateDaemonTaskInfo(daemonTaskInfo);

        Assert.assertEquals(CassandraTemplateTask.toTemplateTaskName(daemonTaskInfo.getName()),
                clusterTemplateTaskInfo.getName());
        Assert.assertEquals(2, clusterTemplateTaskInfo.getResourcesCount());
        Assert.assertTrue(clusterTemplateTaskInfo.getTaskId().getValue().isEmpty());

        for (Protos.Resource resource : clusterTemplateTaskInfo.getResourcesList()) {
            Assert.assertTrue(ResourceUtils.getResourceId(resource).isEmpty());
        }
    }

    @Test
    public void testGetContainerExecutorInfo() throws Exception {
        CassandraContainer container = getTestCassandraContainer();
        Protos.ExecutorInfo execInfo = container.getExecutorInfo();
        validateDaemonExecutorInfo(execInfo);
    }

    @Test
    public void testGetOrCreateCassandraContainer() throws Exception {
        CassandraContainer cassandraContainer = cassandraState.getOrCreateContainer(testDaemonName);
        validateDaemonTaskInfo(cassandraContainer.getDaemonTask().getTaskInfo());
        validateDaemonExecutorInfo(cassandraContainer.getExecutorInfo());
    }

    @Test
    public void testMoveDaemon() throws Exception {
        CassandraDaemonTask originalDaemonTask = cassandraState.createDaemon(testDaemonName);
        originalDaemonTask = originalDaemonTask.update(getTestOffer());
        Assert.assertEquals(testHostName, originalDaemonTask.getHostname());

        CassandraDaemonTask movedDaemonTask = cassandraState.moveDaemon(originalDaemonTask);
        CassandraData movedData = CassandraData.parse(movedDaemonTask.getTaskInfo().getData());
        String movedReplaceIp = movedData.getConfig().getReplaceIp();
        Assert.assertEquals(originalDaemonTask.getHostname(), movedReplaceIp);
    }

    @Test
    public void testUpdateTaskStatus() throws Exception {
        // Test that updating an empty StateStore doesn't have any ill effects
        Assert.assertEquals(0, cassandraState.getDaemons().size());
        cassandraState.update(getTestTaskStatus());
        Assert.assertEquals(0, cassandraState.getDaemons().size());

        // Initial update should result in STAGING state
        CassandraDaemonTask daemonTask = cassandraState.createDaemon(testDaemonName);
        cassandraState.update(daemonTask.getTaskInfo(), getTestOffer());
        Assert.assertEquals(1, cassandraState.getDaemons().size());
        CassandraDaemonTask updatedDaemonTask = cassandraState.getDaemons().entrySet().iterator().next().getValue();
        Assert.assertEquals(Protos.TaskState.TASK_STAGING, updatedDaemonTask.getState());

        // TaskStatus update with RUNNING should result in RUNNING state.
        cassandraState.update(getTestTaskStatus(daemonTask));
        Assert.assertEquals(
                Protos.TaskState.TASK_RUNNING,
                stateStore.fetchStatus(updatedDaemonTask.getName()).get().getState());
    }

    private void validateDaemonTaskInfo(Protos.TaskInfo daemonTaskInfo) throws TaskException {
        Assert.assertEquals(testDaemonName, daemonTaskInfo.getName());
        Assert.assertEquals(4, daemonTaskInfo.getResourcesCount());
        Assert.assertEquals(testDaemonName, TaskUtils.toTaskName(daemonTaskInfo.getTaskId()));
        Assert.assertTrue(daemonTaskInfo.getSlaveId().getValue().isEmpty());

        for (Protos.Resource resource : daemonTaskInfo.getResourcesList()) {
            Assert.assertTrue(ResourceUtils.getResourceId(resource).isEmpty());
        }
    }

    private void validateDaemonExecutorInfo(Protos.ExecutorInfo executorInfo) {
        for (Protos.Resource resource : executorInfo.getResourcesList()) {
            Assert.assertTrue(ResourceUtils.getResourceId(resource).isEmpty());
        }
    }

    private CassandraContainer getTestCassandraContainer() throws Exception{
        CassandraDaemonTask daemonTask = cassandraState.createDaemon(testDaemonName);
        return cassandraState.createCassandraContainer(daemonTask);
    }

    private Protos.Offer getTestOffer() {
        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder()
                        .setValue("test-offer-id")
                        .build())
                .setFrameworkId(Protos.FrameworkID.newBuilder()
                        .setValue("test-framework-id")
                        .build())
                .setSlaveId(Protos.SlaveID.newBuilder()
                        .setValue("test-slave-id")
                        .build())
                .setHostname(testHostName)
                .build();
    }

    private Protos.TaskStatus getTestTaskStatus() {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder()
                        .setValue(testTaskId)
                        .build())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
    }

    private Protos.TaskStatus getTestTaskStatus(CassandraDaemonTask task) {
        final CassandraDaemonStatus status = task.createStatus(Protos.TaskState.TASK_RUNNING, CassandraMode.NORMAL, Optional.empty());
        return status.getTaskStatus();
    }
}
