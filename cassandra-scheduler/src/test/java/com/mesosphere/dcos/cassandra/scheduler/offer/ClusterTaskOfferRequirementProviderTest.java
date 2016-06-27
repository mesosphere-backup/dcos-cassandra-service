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
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.protobuf.TaskInfoBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Status;
import org.junit.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

public class ClusterTaskOfferRequirementProviderTest {

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
    private static CassandraTasks cassandraTasks;
    private static ClusterTaskOfferRequirementProvider provider;
    private static Protos.TaskInfo testTaskInfo;

    private static final String testRole = "cassandra-role";
    private static final String testPrincipal = "cassandra-principal";
    private static final String testResourceId = "cassandra-resource-id";
    private static final Double testCpus = 1.0;
    private static final Double testMem = 1000.0;
    private static final Double testDisk = 2000.0;
    private static final Integer testPortBegin = 7000;
    private static final Integer testPortEnd = 7001;

    @Before
    public void beforeEach() throws Exception {
        cassandraTasks = new CassandraTasks(
                identity,
                configuration,
                curatorConfig,
                clusterTaskConfig,
                CassandraTask.PROTO_SERIALIZER,
                persistence);

        CassandraDaemonTask task = cassandraTasks.createDaemon("test-daemon");
        Protos.TaskInfo initTaskInfo = task.getTaskInfo();

        TaskInfoBuilder builder = new TaskInfoBuilder(
            initTaskInfo.getTaskId().getValue(),
            initTaskInfo.getName(),
            initTaskInfo.getSlaveId().getValue());
        Protos.Resource cpu = ResourceBuilder.reservedCpus(
            testCpus, testRole, testPrincipal, testResourceId);
        Protos.Resource mem = ResourceBuilder.reservedMem(
            testMem, testRole, testPrincipal, testResourceId);
        Protos.Resource disk = ResourceBuilder.reservedDisk(
            testDisk, testRole, testPrincipal, testResourceId);
        Protos.Resource ports = ResourceBuilder.reservedPorts(
            testPortBegin, testPortEnd, testRole, testPrincipal, testResourceId);
        builder.addAllResources(Arrays.asList(cpu, mem, disk, ports));
        builder.setExecutorInfo(initTaskInfo.getExecutor());

        testTaskInfo = builder.build();
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

        provider = new ClusterTaskOfferRequirementProvider(identity, cassandraTasks);
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
    public void testConstructor() throws Exception {
        ClusterTaskOfferRequirementProvider provider = new ClusterTaskOfferRequirementProvider(
            identity, cassandraTasks);
        CassandraDaemonTask task = cassandraTasks.createDaemon("test-daemon");
        Protos.TaskInfo taskInfo = task.getTaskInfo();

        OfferRequirement requirement = provider.getNewOfferRequirement(taskInfo);
        Assert.assertNotNull(requirement);
    }

    @Test
    public void testGetNewOfferRequirement() throws Exception {
        OfferRequirement requirement = provider.getNewOfferRequirement(testTaskInfo);
        Protos.TaskInfo taskInfo = requirement.getTaskRequirements().iterator().next().getTaskInfo();
        Assert.assertEquals(taskInfo.getName(), "test-daemon");
        Assert.assertTrue(taskInfo.getTaskId().getValue().contains("test-daemon"));
        Assert.assertEquals(taskInfo.getSlaveId().getValue(), "");

        List<Protos.Resource> resources = taskInfo.getResourcesList();
        Assert.assertEquals(4, resources.size());

        Protos.Resource cpusResource = resources.get(0);
        Assert.assertEquals("cpus", cpusResource.getName());
        Assert.assertEquals(Protos.Value.Type.SCALAR, cpusResource.getType());
        Assert.assertEquals(testCpus, cpusResource.getScalar().getValue(), 0.0);
        Assert.assertEquals(testRole, cpusResource.getRole());
        Assert.assertEquals(testPrincipal, cpusResource.getReservation().getPrincipal());
        Assert.assertEquals("resource_id", cpusResource.getReservation().getLabels().getLabelsList().get(0).getKey());
        Assert.assertEquals(testResourceId, cpusResource.getReservation().getLabels().getLabelsList().get(0).getValue());

        Protos.Resource memResource = resources.get(1);
        Assert.assertEquals("mem", memResource.getName());
        Assert.assertEquals(Protos.Value.Type.SCALAR, memResource.getType());
        Assert.assertEquals(testMem, memResource.getScalar().getValue(), 0.0);
        Assert.assertEquals(testRole, memResource.getRole());
        Assert.assertEquals(testPrincipal, memResource.getReservation().getPrincipal());
        Assert.assertEquals("resource_id", memResource.getReservation().getLabels().getLabelsList().get(0).getKey());
        Assert.assertEquals(testResourceId, memResource.getReservation().getLabels().getLabelsList().get(0).getValue());

        Protos.Resource diskResource = resources.get(2);
        Assert.assertEquals("disk", diskResource.getName());
        Assert.assertEquals(Protos.Value.Type.SCALAR, diskResource.getType());
        Assert.assertEquals(testDisk, diskResource.getScalar().getValue(), 0.0);
        Assert.assertEquals(testRole, diskResource.getRole());
        Assert.assertEquals(testPrincipal, diskResource.getReservation().getPrincipal());
        Assert.assertEquals("resource_id", diskResource.getReservation().getLabels().getLabelsList().get(0).getKey());
        Assert.assertEquals(testResourceId, diskResource.getReservation().getLabels().getLabelsList().get(0).getValue());

        Protos.Resource portsResource = resources.get(3);
        Assert.assertEquals("ports", portsResource.getName());
        Assert.assertEquals(Protos.Value.Type.RANGES, portsResource.getType());
        Assert.assertTrue(portsResource.getRanges().getRangeList().get(0).getBegin() >= testPortBegin);
        Assert.assertTrue(portsResource.getRanges().getRangeList().get(0).getEnd() >= testPortBegin);
        Assert.assertEquals(testRole, portsResource.getRole());
        Assert.assertEquals(testPrincipal, portsResource.getReservation().getPrincipal());
        Assert.assertEquals("resource_id", portsResource.getReservation().getLabels().getLabelsList().get(0).getKey());
        Assert.assertEquals(testResourceId, portsResource.getReservation().getLabels().getLabelsList().get(0).getValue());

        final Protos.ExecutorInfo executorInfo = requirement.getExecutorRequirement().getExecutorInfo();

        Protos.CommandInfo cmd = executorInfo.getCommand();
        Assert.assertEquals(3, cmd.getUrisList().size());
        Assert.assertEquals(
            config.getExecutorConfig().getExecutorLocation().toString(),
            cmd.getUrisList().get(0).getValue());
        Assert.assertEquals(
            config.getExecutorConfig().getCassandraLocation().toString(),
            cmd.getUrisList().get(1).getValue());
        Assert.assertEquals(
            config.getExecutorConfig().getJreLocation().toString(),
            cmd.getUrisList().get(2).getValue());
    }

    @Test
    public void testGetUpdateOfferRequirement() throws Exception {
        OfferRequirement requirement = provider.getUpdateOfferRequirement(testTaskInfo);
        Protos.TaskInfo taskInfo = requirement.getTaskRequirements().iterator().next().getTaskInfo();
        Assert.assertEquals(taskInfo.getName(), "test-daemon");
        Assert.assertTrue(taskInfo.getTaskId().getValue().contains("test-daemon"));
        Assert.assertEquals("", taskInfo.getSlaveId().getValue());

        List<Protos.Resource> resources = taskInfo.getResourcesList();
        Assert.assertEquals(4, resources.size());

        Protos.Resource cpusResource = resources.get(0);
        Assert.assertEquals("cpus", cpusResource.getName());
        Assert.assertEquals(testCpus, cpusResource.getScalar().getValue(), 0.0);

        Protos.Resource memResource = resources.get(1);
        Assert.assertEquals("mem", memResource.getName());
        Assert.assertEquals(testMem, memResource.getScalar().getValue(), 0.0);

        Protos.Resource diskResource = resources.get(2);
        Assert.assertEquals("disk", diskResource.getName());
        Assert.assertEquals(testDisk, diskResource.getScalar().getValue(), 0.0);

        Protos.Resource portsResource = resources.get(3);
        Assert.assertEquals("ports", portsResource.getName());
        Assert.assertTrue(portsResource.getRanges().getRangeList().get(0).getBegin() >= testPortBegin);
        Assert.assertTrue(portsResource.getRanges().getRangeList().get(0).getEnd() >= testPortBegin);
    }

    @Test
    public void testGetReplacementOfferRequirement() throws Exception {
        OfferRequirement requirement = provider.getReplacementOfferRequirement(testTaskInfo);
        Protos.TaskInfo taskInfo = requirement.getTaskRequirements().iterator().next().getTaskInfo();
        Assert.assertEquals(taskInfo.getName(), "test-daemon");
        Assert.assertTrue(taskInfo.getTaskId().getValue().contains("test-daemon"));
        Assert.assertEquals("", taskInfo.getSlaveId().getValue());

        List<Protos.Resource> resources = taskInfo.getResourcesList();
        Assert.assertEquals(4, resources.size());

        Protos.Resource cpusResource = resources.get(0);
        Assert.assertEquals("cpus", cpusResource.getName());
        Assert.assertEquals(testCpus, cpusResource.getScalar().getValue(), 0.0);

        Protos.Resource memResource = resources.get(1);
        Assert.assertEquals("mem", memResource.getName());
        Assert.assertEquals(testMem, memResource.getScalar().getValue(), 0.0);

        Protos.Resource diskResource = resources.get(2);
        Assert.assertEquals("disk", diskResource.getName());
        Assert.assertEquals(testDisk, diskResource.getScalar().getValue(), 0.0);

        Protos.Resource portsResource = resources.get(3);
        Assert.assertEquals("ports", portsResource.getName());
        Assert.assertTrue(portsResource.getRanges().getRangeList().get(0).getBegin() >= testPortBegin);
        Assert.assertTrue(portsResource.getRanges().getRangeList().get(0).getEnd() >= testPortEnd);
    }
}
