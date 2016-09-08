package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
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
import org.junit.*;

import java.util.List;
import java.util.Map;

public class ConnectionResourceTest {
    private static TestingServer server;

    private static CassandraSchedulerConfiguration config;

    private static DefaultConfigurationManager configurationManager;

    private static CassandraTasks cassandraTasks;
    private static ConfigurationManager configuration;

    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new ConnectionResource(cassandraTasks, configuration)).build();

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
        configuration = new ConfigurationManager(configurationManager);
        ClusterTaskConfig clusterTaskConfig = config.getClusterTaskConfig();
        cassandraTasks = new CassandraTasks(
                configuration,
                curatorConfig,
                clusterTaskConfig,
                stateStore);
    }

    @AfterClass
    public static void afterAll() throws Exception {
        server.close();
        server.stop();
    }

    @Test
    public void testGetConnectEmpty() throws Exception {
        final Map map = resources.client().target("/v1/connection").request().get(Map.class);
        final Object address = map.get("address");
        final Object dns = map.get("dns");
        Assert.assertNotNull(address);
        Assert.assertNotNull(dns);
    }

    @Test
    public void testGetAddressEmpty() throws Exception {
        final List address = resources.client().target("/v1/connection/address").request().get(List.class);
        Assert.assertNotNull(address);
    }

    @Test
    public void testGetDnsEmpty() throws Exception {
        final List dns = resources.client().target("/v1/connection/dns").request().get(List.class);
        Assert.assertNotNull(dns);
    }
}