package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
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
import org.apache.mesos.state.CuratorStateStore;
import org.apache.mesos.state.StateStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IdentityResourceTest {
    private static TestingServer server;
    private static DropwizardConfiguration config;
    private static ConfigurationManager configurationManager;
    private static StateStore stateStore;

    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new IdentityResource(configurationManager)).build();

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
                Resources.getResource("scheduler.yml").getFile());

        Identity initial = config.getSchedulerConfiguration().getIdentity();

        final CuratorFrameworkConfig curatorConfig = config.getSchedulerConfiguration().getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        stateStore = new CuratorStateStore(
                "/" + config.getSchedulerConfiguration().getName(),
                server.getConnectString(),
                retryPolicy);

        final CassandraSchedulerConfiguration configuration = config.getSchedulerConfiguration();
        try {
            final ConfigValidator configValidator = new ConfigValidator();
            final DefaultConfigurationManager defaultConfigurationManager
                    = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                    "/" + configuration.getName(),
                    server.getConnectString(),
                    configuration,
                    configValidator);
            configurationManager = new ConfigurationManager(defaultConfigurationManager);
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
        Identity identity = resources.client().target("/v1/framework").request()
                .get(Identity.class);
        System.out.println("identity = " + identity);
        assertEquals(config.getSchedulerConfiguration().getIdentity(), identity);
    }
}
