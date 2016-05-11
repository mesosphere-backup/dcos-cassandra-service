package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class IdentityManagerTest {

    private static TestingServer server;

    private static CuratorFramework curator;

    private static ZooKeeperPersistence persistence;

    private static String path;

    private static Identity initial;


    @BeforeClass
    public static void beforeAll() throws Exception {

        server = new TestingServer();

        server.start();

        final ConfigurationFactory<CassandraSchedulerConfiguration> factory =
                new ConfigurationFactory<>(
                        CassandraSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");

        CassandraSchedulerConfiguration config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());

        initial = config.getIdentity();

        persistence = (ZooKeeperPersistence) ZooKeeperPersistence.create(
                initial,
                CuratorFrameworkConfig.create(server.getConnectString(),
                        10000L,
                        10000L,
                        Optional.empty(),
                        250L));

        curator = persistence.getCurator();

        path = "/cassandra/" + config.getIdentity().getName() +"/identity";


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

    @Test
    public void createIdentity() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

        assertEquals(manager.get(), initial);

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial);
    }

    @Test
    public void registerIdentity() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

        assertEquals(manager.get(), initial);

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial.register("test_id"));

        manager.stop();

        manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

        assertEquals(manager.get(), initial.register("test_id"));

    }

    @Test(expected = IllegalStateException.class)
    public void immutableName() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

        assertEquals(manager.get(), initial);

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial.register("test_id"));

        manager.stop();

        manager = new IdentityManager(
                Identity.create(
                        "foo",
                        initial.getId(),
                        initial.getVersion(),
                        initial.getUser(),
                        initial.getCluster(),
                        initial.getRole(),
                        initial.getPrincipal(),
                        initial.getFailoverTimeout(),
                        initial.getSecret(),
                        initial.isCheckpoint()),
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

    }

    @Test(expected = IllegalStateException.class)
    public void immutableCluster() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

        assertEquals(manager.get(), initial);

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial.register("test_id"));

        manager.stop();

        manager = new IdentityManager(
                Identity.create(
                        initial.getName(),
                        initial.getId(),
                        initial.getVersion(),
                        initial.getUser(),
                        "foo",
                        initial.getRole(),
                        initial.getPrincipal(),
                        initial.getFailoverTimeout(),
                        initial.getSecret(),
                        initial.isCheckpoint()),
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

    }

    @Test(expected = IllegalStateException.class)
    public void immutableRole() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

        assertEquals(manager.get(), initial);

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial.register("test_id"));

        manager.stop();

        manager = new IdentityManager(
                Identity.create(
                        initial.getName(),
                        initial.getId(),
                        initial.getVersion(),
                        initial.getUser(),
                        initial.getCluster(),
                        "foo",
                        initial.getPrincipal(),
                        initial.getFailoverTimeout(),
                        initial.getSecret(),
                        initial.isCheckpoint()),
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

    }

    @Test(expected = IllegalStateException.class)
    public void immutablePrincipal() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

        assertEquals(manager.get(), initial);

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

        assertEquals(
                Identity.JSON_SERIALIZER.deserialize(curator.getData().forPath(path)),
                initial.register("test_id"));

        manager.stop();

        manager = new IdentityManager(
                Identity.create(
                        initial.getName(),
                        initial.getId(),
                        initial.getVersion(),
                        initial.getUser(),
                        initial.getCluster(),
                        initial.getRole(),
                        "foo",
                        initial.getFailoverTimeout(),
                        initial.getSecret(),
                        initial.isCheckpoint()),
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

    }
}
