package com.mesosphere.dcos.cassandra.scheduler.tasks;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTemplateTask;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.junit.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

/**
 * Created by kowens on 2/8/16.
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
    public void testCassandraContainerCreate() throws Exception {
        CassandraContainer container = getTestCassandraContainer();
        Assert.assertNotNull(container);
    }

    @Test
    public void testGetContainerTaskInfos() throws Exception {
        CassandraContainer container = getTestCassandraContainer();
        Collection<Protos.TaskInfo> taskInfos = container.getTaskInfos();
        Assert.assertEquals(2, taskInfos.size());

        Iterator<Protos.TaskInfo> iter = taskInfos.iterator();
        Protos.TaskInfo daemonTaskInfo = iter.next();
        Protos.TaskInfo clusterTemplateTaskInfo = iter.next();

        Assert.assertEquals(testDaemonName, daemonTaskInfo.getName());
        Assert.assertEquals(-1, );




        Assert.assertEquals(CassandraTemplateTask.CLUSTER_TASK_TEMPLATE_NAME, clusterTemplateTaskInfo.getName());
    }

    @Test
    public void testGetContainerExecutorInfo() throws Exception {
        CassandraContainer container = getTestCassandraContainer();
        Protos.ExecutorInfo execInfo = container.getExecutorInfo();
        Assert.assertNotNull(execInfo);
    }

    @Test
    public void testGetOrCreateCassandraContainer() throws PersistenceException {
        Assert.assertNotNull(cassandraTasks.getOrCreateContainer("test_name"));
    }

    @Test
    public void testCreateCassandraContainer() throws Exception {
        CassandraDaemonTask daemonTask = cassandraTasks.createDaemon(testDaemonName);
        CassandraContainer container = cassandraTasks.createCassandraContainer(daemonTask);
        Assert.assertNotNull(container);
    }

    private CassandraContainer getTestCassandraContainer() throws Exception{
        CassandraDaemonTask daemonTask = cassandraTasks.createDaemon(testDaemonName);
        CassandraTemplateTask clusterTemplateTask = CassandraTemplateTask.create(
                "test_role",
                "test_principal",
                config.getClusterTaskConfig());
        return CassandraContainer.create(daemonTask, clusterTemplateTask);
    }
}
