package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
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
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.state.StateStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class ServiceConfigResourceTest {
    private static TestingServer server;
    private static MutableSchedulerConfiguration config;
    private static ConfigurationManager configurationManager;
    private static StateStore stateStore;

    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new ServiceConfigResource(configurationManager)).build();

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

        config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());

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
                config.createConfig().getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);

        final CassandraSchedulerConfiguration configuration = config.createConfig();
        try {
            final ConfigValidator configValidator = new ConfigValidator();
            final DefaultConfigurationManager defaultConfigurationManager =
                    new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                    configuration.getServiceConfig().getName(),
                    server.getConnectString(),
                    configuration,
                    configValidator,
                    stateStore);
            Capabilities mockCapabilities = Mockito.mock(Capabilities.class);
            when(mockCapabilities.supportsNamedVips()).thenReturn(true);
            configurationManager = new ConfigurationManager(
                    new CassandraDaemonTask.Factory(mockCapabilities),
                    defaultConfigurationManager);
        } catch (ConfigStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void afterAll() throws Exception {
        server.close();
        server.stop();
    }

    @Test
    public void testGetIdentity() throws Exception {
        ServiceConfig serviceConfig = resources.client().target("/v1/framework").request()
                .get(ServiceConfig.class);
        System.out.println("serviceConfig = " + serviceConfig);
        assertEquals(config.getServiceConfig(), serviceConfig);
    }
}
