package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
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

import static org.junit.Assert.assertEquals;

public class ConfigurationResourceTest {
    private static TestingServer server;

    private static CassandraSchedulerConfiguration config;

    private static DefaultConfigurationManager configurationManager;
    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new ConfigurationResource(configurationManager)).build();

    @BeforeClass
    public static void beforeAll() throws Exception {
        server = new TestingServer();
        server.start();
        final ConfigurationFactory<DropwizardConfiguration> factory =
                new ConfigurationFactory<>(
                        DropwizardConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(
                                new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");
        config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile()).getSchedulerConfiguration();
        configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + config.getName(),
                server.getConnectString(),
                config,
                new ConfigValidator());
        config = (CassandraSchedulerConfiguration) configurationManager.getTargetConfig();
    }

    @AfterClass
    public static void afterAll() throws Exception {
        server.close();
        server.stop();
    }

    @Test
    public void testGetServers() throws Exception {
        Integer servers = resources.client().target("/v1/config/nodes").request()
                .get(Integer.class);
        System.out.println("servers = " + servers);
        assertEquals(config.getServers(), servers.intValue());
    }

    @Test
    public void testGetCassandraConfig() throws Exception {
        CassandraConfig cassandraConfig = resources.client().target("/v1/config/cassandra").request()
                .get(CassandraConfig.class);
        System.out.println("cassandra config = " + cassandraConfig);
        assertEquals(config.getCassandraConfig(), cassandraConfig);
    }

    @Test
    public void testGetExecutorConfig() throws Exception {
        ExecutorConfig executorConfig = resources.client().target("/v1/config/executor").request()
                .get(ExecutorConfig.class);
        System.out.println("executor config = " + executorConfig);
        assertEquals(config.getExecutorConfig(), executorConfig);
    }

    @Test
    public void testGetSeeds() throws Exception {
        Integer seeds = resources.client().target("/v1/config/seed-nodes").request()
                .get(Integer.class);
        System.out.println("seeds = " + seeds);
        assertEquals(config.getSeeds(), seeds.intValue());
    }
}
