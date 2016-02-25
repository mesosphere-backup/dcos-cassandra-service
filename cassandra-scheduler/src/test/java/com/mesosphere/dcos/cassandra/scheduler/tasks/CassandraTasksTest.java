package com.mesosphere.dcos.cassandra.scheduler.tasks;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.TestData;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
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
import org.apache.mesos.protobuf.OfferBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

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

    private static String path;

    private static String path(String id){

        return path + "/" + id;
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
                        new EnvironmentVariableSubstitutor(false)),
                Resources.getResource("scheduler.yml").getFile());

        Identity initial = config.getIdentity();

        persistence = (ZooKeeperPersistence) ZooKeeperPersistence.create(
                initial,
                CuratorFrameworkConfig.create(server.getConnectString(),
                        10000L,
                        10000L,
                        Optional.empty(),
                        250L));

        curator = persistence.getCurator();

        identity = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        identity.register("test_id");

        configuration = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        path = "/" + config.getIdentity().getName() + "/" +
                config.getIdentity().getCluster() + "/tasks";
    }

    @Test
    public void createTask() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        CassandraDaemonTask task = tasks.createDaemon(
                CassandraDaemonTask.NAME_PREFIX + 0);


        assertEquals(tasks.get(task.getName()).get(),task);

        assertEquals(CassandraTask.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(path(task.getName()))
        ),task);
    }

    @Test
    public void retrieveTaskOnRestart() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        CassandraDaemonTask task = tasks.createDaemon(
        CassandraDaemonTask.NAME_PREFIX + 0);

        assertEquals(tasks.get(task.getName()).get(),task);

        assertEquals(CassandraTask.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(path(task.getName()))
        ),task);

        tasks.stop();

        tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();


        assertEquals(tasks.get(CassandraDaemonTask.NAME_PREFIX + 0).get(),task);
    }

    @Test
    public void protoSerializable() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        CassandraDaemonTask task = tasks.createDaemon(CassandraDaemonTask.NAME_PREFIX + 0);

        assertEquals(CassandraDaemonTask.parse(task.toProto()),task);
    }

    @Test
    public void updateTaskWithOffer() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        Protos.Offer offer = TestData.createOffer(
                TestData.randomId(),
                "slave",
                config.getIdentity().getName(),
                "localhost",
                Arrays.asList(
                        ResourceBuilder.reservedCpus(
                                config.getCassandraConfig().getCpus(),
                                config.getIdentity().getRole(),
                                config.getIdentity().getPrincipal()),
                        ResourceBuilder.reservedMem(
                                config.getCassandraConfig().getDiskMb(),
                                config.getIdentity().getRole(),
                                config.getIdentity().getPrincipal()),
                        ResourceBuilder.reservedDisk(
                                config.getCassandraConfig().getDiskMb(),
                                config.getIdentity().getRole(),
                                config.getIdentity().getPrincipal())
                )
        );

        CassandraDaemonTask task = tasks.createDaemon(CassandraDaemonTask.NAME_PREFIX + 0);


        assertEquals(CassandraDaemonTask.parse(task.toProto()),task);

        CassandraTask updated = tasks.update(task.getId(),offer).get();

        assertEquals(updated.getSlaveId(),"slave");

        assertEquals(updated.getHostname(),"localhost");

        assertEquals(CassandraTask.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(path(task.getName()))
        ),updated);

    }

    @After
    public void afterEach() {

        try {
            curator.delete().forPath(path);
        } catch (Exception e) {

        }
    }

    @AfterClass
    public static void afterAll() throws Exception {

        persistence.stop();

        server.close();

        server.stop();
    }
}
