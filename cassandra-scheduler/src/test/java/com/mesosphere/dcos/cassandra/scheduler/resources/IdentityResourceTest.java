package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.scheduler.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.scheduler.config.CuratorFrameworkConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit.ResourceTestRule;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class IdentityResourceTest {
    private static TestingServer server;

    private static ZooKeeperPersistence persistence;

    private static CassandraSchedulerConfiguration config;

    private static IdentityManager manager;

    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new IdentityResource(manager)).build();

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

        persistence = (ZooKeeperPersistence) ZooKeeperPersistence.create(
                initial,
                CuratorFrameworkConfig.create(server.getConnectString(),
                        10000L,
                        10000L,
                        Optional.empty(),
                        250L));

        manager = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        manager.start();

    }

    @AfterClass
    public static void afterAll() throws Exception {

        manager.stop();

        persistence.stop();

        server.close();

        server.stop();
    }

    @Test
    public void testGetIdentity() throws Exception {
        Identity identity = resources.client().target("/v1/framework").request()
                .get(Identity.class);
        System.out.println("identity = " + identity);
        assertEquals(config.getIdentity(), identity);
    }
}
