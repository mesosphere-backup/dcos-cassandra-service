package com.mesosphere.dcos.cassandra.scheduler.tasks;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.offer.TaskUtils;
import org.junit.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

/**
 * This class tests the CassandraTasks class.
 */
public class CassandraTasksTest {

    private static TestingServer server;
    private static CuratorFramework curator;
    private static ZooKeeperPersistence persistence;
    private static CassandraSchedulerConfiguration config;
    private static IdentityManager identity;
    private static ConfigurationManager configuration;
    private static CuratorFrameworkConfig curatorConfig;
    private static ClusterTaskConfig clusterTaskConfig;
    private static String path;
    private static String testDaemonName = "test-daemon-name";
    private static String testHostName = "test-host-name";
    private static String testTaskId = "test-task-id__UUID";
    private CassandraTasks cassandraTasks;

    @Before
    public void beforeEach() throws Exception {
        cassandraTasks = new CassandraTasks(
                identity,
                configuration,
                curatorConfig,
                clusterTaskConfig,
                CassandraTask.PROTO_SERIALIZER,
                persistence);
    }

    @BeforeClass
    public static void beforeAll() throws Exception {

        server = new TestingServer();

        server.start();

        final ConfigurationFactory<CassandraSchedulerConfiguration> factory =
                new ConfigurationFactory<>(
                        CassandraSchedulerConfiguration.class,
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

        Identity initial = config.getIdentity();

        curatorConfig = CuratorFrameworkConfig.create(server.getConnectString(),
                10000L,
                10000L,
                Optional.empty(),
                250L);

        clusterTaskConfig = config.getClusterTaskConfig();

        persistence = (ZooKeeperPersistence) ZooKeeperPersistence.create(
                initial,
                curatorConfig);

        curator = persistence.getCurator();

        identity = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        identity.register("test_id");

        configuration = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getClusterTaskConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                "NODE",
                config.getSeedsUrl(),
                config.getDcUrl(),
                config.getExternalDcsList(),
                config.getExternalDcSyncMs(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                ClusterTaskConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        path = "/cassandra/" + config.getIdentity().getName() +"/tasks";
    }

    @After
    public void afterEach() {

        try {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {

        }
    }

    @AfterClass
    public static void afterAll() throws Exception {
        persistence.stop();
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

        Assert.assertEquals(CassandraTemplateTask.CLUSTER_TASK_TEMPLATE_NAME, clusterTemplateTaskInfo.getName());
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
        CassandraContainer cassandraContainer = cassandraTasks.getOrCreateContainer(testDaemonName);
        validateDaemonTaskInfo(cassandraContainer.getDaemonTask().getTaskInfo());
        validateDaemonExecutorInfo(cassandraContainer.getExecutorInfo());
    }

    @Test
    public void testMoveDaemon() throws Exception {
        CassandraDaemonTask originalDaemonTask = cassandraTasks.createDaemon(testDaemonName);
        originalDaemonTask = originalDaemonTask.update(getTestOffer());
        Assert.assertEquals(testHostName, originalDaemonTask.getHostname());

        CassandraDaemonTask movedDaemonTask = cassandraTasks.moveDaemon(originalDaemonTask);
        CassandraData movedData = CassandraData.parse(movedDaemonTask.getTaskInfo().getData());
        String movedReplaceIp = movedData.getConfig().getReplaceIp();
        Assert.assertEquals(originalDaemonTask.getHostname(), movedReplaceIp);
    }

    @Test
    public void testUpdateTaskStatus() throws Exception {
        // Test that updating an empty StateStore doesn't have any ill effects
        Assert.assertEquals(0, cassandraTasks.getDaemons().size());
        cassandraTasks.update(getTestTaskStatus());
        Assert.assertEquals(0, cassandraTasks.getDaemons().size());

        // Initial update should result in STAGING state
        CassandraDaemonTask daemonTask = cassandraTasks.createDaemon(testDaemonName);
        cassandraTasks.update(daemonTask.getTaskInfo(), getTestOffer());
        Assert.assertEquals(1, cassandraTasks.getDaemons().size());
        CassandraDaemonTask updatedDaemonTask = cassandraTasks.getDaemons().entrySet().iterator().next().getValue();
        Assert.assertEquals(Protos.TaskState.TASK_STAGING, updatedDaemonTask.getState());

        // TaskStatus update with RUNNING should result in RUNNING state.
        cassandraTasks.update(getTestTaskStatus(daemonTask.getTaskInfo().getTaskId().getValue()));
        updatedDaemonTask = cassandraTasks.getDaemons().entrySet().iterator().next().getValue();
        Assert.assertEquals(Protos.TaskState.TASK_RUNNING, updatedDaemonTask.getState());
    }

    private void validateDaemonTaskInfo(Protos.TaskInfo daemonTaskInfo) {
        Assert.assertEquals(testDaemonName, daemonTaskInfo.getName());
        Assert.assertEquals(4, daemonTaskInfo.getResourcesCount());
        try {
            Assert.assertEquals(testDaemonName, TaskUtils.toTaskName(daemonTaskInfo.getTaskId()));
        } catch (TaskException e) {
            Assert.assertTrue(e == null);
        }
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
        CassandraDaemonTask daemonTask = cassandraTasks.createDaemon(testDaemonName);
        return cassandraTasks.createCassandraContainer(daemonTask);
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
        return getTestTaskStatus(testTaskId);
    }

    private Protos.TaskStatus getTestTaskStatus(String taskId) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder()
                        .setValue(taskId)
                        .build())
                .setExecutorId(Protos.ExecutorID.newBuilder()
                        .setValue(taskId)
                        .build())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
    }
}
