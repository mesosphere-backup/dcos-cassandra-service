package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.*;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit.ResourceTestRule;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.state.StateStore;
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
        final ConfigurationFactory<MutableSchedulerConfiguration> factory =
                new ConfigurationFactory<>(
                        MutableSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(
                                new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");
        MutableSchedulerConfiguration mutable = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        config = mutable.createConfig();

        final CuratorFrameworkConfig curatorConfig = mutable.getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        StateStore stateStore = new CuratorStateStore(
                config.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                config.getServiceConfig().getName(),
                server.getConnectString(),
                config,
                new ConfigValidator(),
                stateStore);
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
