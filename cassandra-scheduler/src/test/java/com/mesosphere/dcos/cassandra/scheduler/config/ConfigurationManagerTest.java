package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
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
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.state.StateStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ConfigurationManagerTest {
    private static TestingServer server;
    private static ConfigurationFactory<MutableSchedulerConfiguration> factory;
    private static String connectString;

    @Before
    public void beforeAll() throws Exception {
        server = new TestingServer();
        server.start();
        factory = new ConfigurationFactory<>(
                        MutableSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper()
                                .registerModule(new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");
        connectString = server.getConnectString();
    }

    @AfterClass
    public static void afterAll() throws Exception {
        server.close();
        server.stop();
    }

    @Test
    public void loadAndPersistConfiguration() throws Exception {
        final String configFilePath = Resources.getResource("scheduler.yml").getFile();
        MutableSchedulerConfiguration mutableConfig = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                configFilePath);
        final CassandraSchedulerConfiguration original  = mutableConfig.createConfig();
        final CuratorFrameworkConfig curatorConfig = mutableConfig.getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        StateStore stateStore = new CuratorStateStore(
                original.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        DefaultConfigurationManager configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                original.getServiceConfig().getName(),
                connectString,
                original,
                new ConfigValidator(),
                stateStore);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();
        assertEquals("cassandra", original.getServiceConfig().getName());
        assertEquals("cassandra-role", original.getServiceConfig().getRole());
        assertEquals("cassandra-cluster", original.getServiceConfig().getCluster());
        assertEquals("cassandra-principal",
                original.getServiceConfig().getPrincipal());
        assertEquals("", original.getServiceConfig().getSecret());

        manager.start();

        assertEquals(original.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(original.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(original.getServers(), targetConfig.getServers());
        assertEquals(original.getSeeds(), targetConfig.getSeeds());
    }


    @Test
    public void applyConfigUpdate() throws Exception {
        MutableSchedulerConfiguration mutable = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        final CassandraSchedulerConfiguration original = mutable.createConfig();
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
                original.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        DefaultConfigurationManager configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                original.getServiceConfig().getName(),
                connectString,
                original,
                new ConfigValidator(),
                stateStore);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig =
          (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(original.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(original.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(original.getServers(), targetConfig.getServers());
        assertEquals(original.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        ExecutorConfig updatedExecutorConfig = new ExecutorConfig(
                "/command/line",
                new ArrayList<>(),
                1.2,
                345,
                901,
                17,
                "/java/home",
                URI.create("/jre/location"), URI.create("/executor/location"),
                URI.create("/cassandra/location"));
        int updatedServers = original.getServers() + 10;
        int updatedSeeds = original.getSeeds() + 5;

        mutable = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());


        mutable.setSeeds(updatedSeeds);
        mutable.setServers(updatedServers);
        mutable.setExecutorConfig(updatedExecutorConfig);

        mutable.setCassandraConfig(
          mutable.getCassandraConfig()
        .mutable().setJmxPort(8000).setCpus(0.6).setMemoryMb(10000).build());

        CassandraSchedulerConfiguration updated = mutable.createConfig();

        configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                original.getServiceConfig().getName(),
                connectString,
                updated,
                new ConfigValidator(),
                stateStore);
        configurationManager.store(updated);
        manager = new ConfigurationManager(configurationManager);
        targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(updated.getCassandraConfig(), targetConfig.getCassandraConfig());

        assertEquals(updatedExecutorConfig, targetConfig.getExecutorConfig());

        assertEquals(updatedServers, targetConfig.getServers());

        assertEquals(updatedSeeds, targetConfig.getSeeds());
    }

    @Test
    public void failOnBadServersCount() throws Exception {
        MutableSchedulerConfiguration mutable = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration originalConfig = mutable.createConfig();
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
                originalConfig.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        DefaultConfigurationManager configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                originalConfig.getServiceConfig().getName(),
                connectString,
                originalConfig,
                new ConfigValidator(),
                stateStore);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(originalConfig.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfig.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfig.getServers(), targetConfig.getServers());
        assertEquals(originalConfig.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        int updatedServers = originalConfig.getServers() - 1;
        mutable.setServers(updatedServers);

        configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                originalConfig.getServiceConfig().getName(),
                connectString,
                mutable.createConfig(),
                new ConfigValidator(),
                stateStore);
        manager = new ConfigurationManager(configurationManager);

        manager.start();

        assertEquals(1, configurationManager.getErrors().size());
    }

    @Test
    public void failOnBadSeedsCount() throws Exception {
        MutableSchedulerConfiguration mutableSchedulerConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration originalConfig = mutableSchedulerConfiguration.createConfig();
        final CuratorFrameworkConfig curatorConfig = mutableSchedulerConfiguration.getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        StateStore stateStore = new CuratorStateStore(
                originalConfig.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        DefaultConfigurationManager configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                originalConfig.getServiceConfig().getName(),
                connectString,
                originalConfig,
                new ConfigValidator(),
                stateStore);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(originalConfig.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfig.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfig.getServers(), targetConfig.getServers());
        assertEquals(originalConfig.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        int updatedSeeds = originalConfig.getServers() + 1;
        mutableSchedulerConfiguration.setSeeds(updatedSeeds);

        configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                originalConfig.getServiceConfig().getName(),
                connectString,
                mutableSchedulerConfiguration.createConfig(),
                new ConfigValidator(),
                stateStore);
        manager = new ConfigurationManager(configurationManager);
        manager.start();
        assertEquals(1, configurationManager.getErrors().size());
    }
}
