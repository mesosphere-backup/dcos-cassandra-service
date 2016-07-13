package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.state.CuratorStateStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class IdentityManagerTest {
    private static TestingServer server;
    private static Identity initial;
    private static CuratorStateStore curatorStateStore;

    @Before
    public void beforeEach() throws Exception {
        server = new TestingServer();
        server.start();

        final ConfigurationFactory<DropwizardConfiguration> factory =
                new ConfigurationFactory<>(
                        DropwizardConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");

        CassandraSchedulerConfiguration configuration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile()).getSchedulerConfiguration();

        final CuratorFrameworkConfig curatorConfig = configuration.getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        curatorStateStore = new CuratorStateStore(
                "/" + configuration.getName(),
                server.getConnectString(),
                retryPolicy);

        initial = configuration.getIdentity();
    }

    @After
    public void afterEach() throws IOException {
        server.close();
        server.stop();
    }

    @Test
    public void createIdentity() throws Exception {
        IdentityManager manager = new IdentityManager(
                initial,
                curatorStateStore);
        manager.start();
        assertEquals(manager.get(), initial);
    }

    @Test
    public void registerIdentity() throws Exception {
        IdentityManager manager = new IdentityManager(
                initial,
                curatorStateStore);
        manager.start();
        assertEquals(manager.get(), initial);

        manager.register("test_id");
        assertEquals(manager.get(), initial.register("test_id"));

        manager.stop();

        manager = new IdentityManager(
                initial,
                curatorStateStore);
        manager.start();
        assertEquals(manager.get(), initial.register("test_id"));
    }

    @Test(expected = IllegalStateException.class)
    public void immutableName() throws Exception {
        IdentityManager manager = new IdentityManager(
                initial,
                curatorStateStore);

        manager.start();
        assertEquals(manager.get(), initial);

        manager.register("test_id");
        assertEquals(manager.get(), initial.register("test_id"));
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
                curatorStateStore);

        manager.start();
    }

    @Test(expected = IllegalStateException.class)
    public void immutableCluster() throws Exception {
        IdentityManager manager = new IdentityManager(
                initial,
                curatorStateStore);
        manager.start();
        assertEquals(manager.get(), initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

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
                curatorStateStore);

        manager.start();

    }

    @Test(expected = IllegalStateException.class)
    public void immutableRole() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                curatorStateStore);

        manager.start();

        assertEquals(manager.get(), initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

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
                curatorStateStore);

        manager.start();

    }

    @Test(expected = IllegalStateException.class)
    public void immutablePrincipal() throws Exception {

        IdentityManager manager = new IdentityManager(
                initial,
                curatorStateStore);

        manager.start();

        assertEquals(manager.get(), initial);

        manager.register("test_id");

        assertEquals(manager.get(), initial.register("test_id"));

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
                curatorStateStore);

        manager.start();

    }
}
