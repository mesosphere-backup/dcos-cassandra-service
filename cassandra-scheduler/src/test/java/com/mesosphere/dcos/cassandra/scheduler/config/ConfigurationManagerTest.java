package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class ConfigurationManagerTest {
    private static TestingServer server;
    private static ConfigurationFactory<DropwizardConfiguration> factory;
    private static String connectString;

    @Before
    public void beforeAll() throws Exception {
        server = new TestingServer();
        server.start();
        factory = new ConfigurationFactory<>(
                        DropwizardConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper()
                                .registerModule(new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");
        connectString = server.getConnectString();
    }

    @Test
    public void loadAndPersistConfiguration() throws Exception {
        final String configFilePath = Resources.getResource("scheduler.yml").getFile();
        DropwizardConfiguration dropwizardConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                configFilePath);
        final CassandraSchedulerConfiguration originalConfiguration = dropwizardConfiguration.getSchedulerConfiguration();
        DefaultConfigurationManager configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + originalConfiguration.getName(),
                connectString,
                originalConfiguration,
                new ConfigValidator());
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        assertEquals("http://cassandra.marathon.mesos:8080/v1/seeds", originalConfiguration.getSeedsUrl());
        assertEquals("cassandra", originalConfiguration.getIdentity().getName());
        assertEquals("cassandra-role", originalConfiguration.getIdentity().getRole());
        assertEquals("cassandra-cluster", originalConfiguration.getIdentity().getCluster());
        assertEquals("cassandra-principal",
                originalConfiguration.getIdentity().getPrincipal());
        assertEquals("", originalConfiguration.getIdentity().getSecret());

        manager.start();

        assertEquals(originalConfiguration.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfiguration.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfiguration.getServers(), targetConfig.getServers());
        assertEquals(originalConfiguration.getSeeds(), targetConfig.getSeeds());
    }


    @Test
    public void applyConfigUpdate() throws Exception {
        DropwizardConfiguration dropwizardConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration originalConfig = dropwizardConfiguration.getSchedulerConfiguration();
        DefaultConfigurationManager configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + originalConfig.getName(),
                connectString,
                originalConfig,
                new ConfigValidator());
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(originalConfig.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfig.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfig.getServers(), targetConfig.getServers());
        assertEquals(originalConfig.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        CassandraConfig updatedCassandraConfig =
                originalConfig.getCassandraConfig().mutable().setJmxPort(8000)
                        .setCpus(0.6).setMemoryMb(10000).build();

        ExecutorConfig updatedExecutorConfig = new ExecutorConfig(
                "/command/line",
                new ArrayList<>(),
                1.2,
                345,
                901,
                17,
                "/java/home",
                URI.create("/jre/location"), URI.create("/executor/location"),
                URI.create("/cassandra/location"),
                "unlimited",
                "100000",
                "32768");
        int updatedServers = originalConfig.getServers() + 10;
        int updatedSeeds = originalConfig.getSeeds() + 5;

        dropwizardConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration updatedConfig = dropwizardConfiguration.getSchedulerConfiguration();

        updatedConfig.setSeeds(updatedSeeds);
        updatedConfig.setServers(updatedServers);
        updatedConfig.setExecutorConfig(updatedExecutorConfig);

        final CassandraConfigParser updatedCassandraConfigParser = updatedConfig.getCassandraConfigParser();
        updatedCassandraConfigParser.setJmxPort(8000);
        updatedCassandraConfigParser.setCpus(0.6);
        updatedCassandraConfigParser.setMemoryMb(10000);

        updatedConfig.setCassandraConfigParser(updatedCassandraConfigParser);

        configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + originalConfig.getName(),
                connectString,
                updatedConfig,
                new ConfigValidator());
        configurationManager.store(updatedConfig);
        manager = new ConfigurationManager(configurationManager);
        targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(updatedCassandraConfig, targetConfig.getCassandraConfig());

        assertEquals(updatedExecutorConfig, targetConfig.getExecutorConfig());

        assertEquals(updatedServers, targetConfig.getServers());

        assertEquals(updatedSeeds, targetConfig.getSeeds());
    }

    @Test
    public void failOnBadServersCount() throws Exception {
        DropwizardConfiguration dropwizardConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration originalConfig = dropwizardConfiguration.getSchedulerConfiguration();
        DefaultConfigurationManager configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + originalConfig.getName(),
                connectString,
                originalConfig,
                new ConfigValidator());
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(originalConfig.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfig.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfig.getServers(), targetConfig.getServers());
        assertEquals(originalConfig.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        int updatedServers = originalConfig.getServers() - 1;
        originalConfig.setServers(updatedServers);

        configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + originalConfig.getName(),
                connectString,
                originalConfig,
                new ConfigValidator());
        manager = new ConfigurationManager(configurationManager);

        manager.start();

        assertEquals(1, configurationManager.getErrors().size());
    }

    @Test
    public void failOnBadSeedsCount() throws Exception {
        DropwizardConfiguration dropwizardConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration originalConfig = dropwizardConfiguration.getSchedulerConfiguration();
        DefaultConfigurationManager configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + originalConfig.getName(),
                connectString,
                originalConfig,
                new ConfigValidator());
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(originalConfig.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfig.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfig.getServers(), targetConfig.getServers());
        assertEquals(originalConfig.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        int updatedSeeds = originalConfig.getServers() + 1;
        originalConfig.setSeeds(updatedSeeds);

        configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + originalConfig.getName(),
                connectString,
                originalConfig,
                new ConfigValidator());
        manager = new ConfigurationManager(configurationManager);
        manager.start();
        assertEquals(1, configurationManager.getErrors().size());
    }

    @After
    public void afterAll() throws Exception {
        server.close();
        server.stop();
    }
}
