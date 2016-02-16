package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
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

public class ConfigurationManagerTest {

    private static TestingServer server;

    private static CuratorFramework curator;

    private static ZooKeeperPersistence persistence;

    private static String cassandraConfigPath;

    private static String executorConfigPath;

    private static String serversPath;

    private static String seedsPath;

    private static CassandraSchedulerConfiguration config;


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
                        new EnvironmentVariableSubstitutor(false)),
                Resources.getResource("scheduler.yml").getFile());

        Identity initial = config.getIdentity();

        persistence = (ZooKeeperPersistence) ZooKeeperPersistence.create(
                initial,
                CuratorFrameworkConfig.create(server.getConnectString(),
                        10000L,
                        10000L,
                        Optional.empty(),
                        250L));

        curator = persistence.getCurator();

        System.out.println(JsonUtils.MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(config));

        cassandraConfigPath = "/" + config.getIdentity().getName() + "/" +
                config.getIdentity().getCluster() + "/cassandraConfig";

        executorConfigPath = "/" + config.getIdentity().getName() + "/" +
                config.getIdentity().getCluster() + "/executorConfig";

        serversPath = "/" + config.getIdentity().getName() + "/" +
                config.getIdentity().getCluster() + "/servers";

        seedsPath = "/" + config.getIdentity().getName() + "/" +
                config.getIdentity().getCluster() + "/seeds";

    }

    @Test
    public void loadAndPersistConfiguration() throws Exception {

        ConfigurationManager manager = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();

        assertEquals(config.getCassandraConfig(), manager.getCassandraConfig());

        assertEquals(config.getExecutorConfig(), manager.getExecutorConfig());

        assertEquals(config.getServers(), manager.getServers());

        assertEquals(config.getSeeds(), manager.getSeeds());

        assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(cassandraConfigPath)),
                config.getCassandraConfig());

        assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(executorConfigPath)),
                config.getExecutorConfig());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(serversPath)).intValue(),
                config.getServers());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(seedsPath)).intValue(),
                config.getSeeds());

    }

    @Test
    public void usePersistedWhenUpdateConfigIsFalse() throws Exception {

        ConfigurationManager manager = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();

        assertEquals(config.getCassandraConfig(), manager.getCassandraConfig());

        assertEquals(config.getExecutorConfig(), manager.getExecutorConfig());

        assertEquals(config.getServers(), manager.getServers());

        assertEquals(config.getSeeds(), manager.getSeeds());

        assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(cassandraConfigPath)),
                config.getCassandraConfig());

        assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(executorConfigPath)),
                config.getExecutorConfig());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(serversPath)).intValue(),
                config.getServers());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(seedsPath)).intValue(),
                config.getSeeds());

        manager.stop();

        manager = new ConfigurationManager(
                config.getCassandraConfig().mutable().setJmxPort(8000)
                        .setCpus(3.0).setMemoryMb(10000).build(),
                config.getExecutorConfig(),
                config.getServers() + 10,
                config.getSeeds() + 10,
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();


        assertEquals(config.getCassandraConfig(), manager.getCassandraConfig());

        assertEquals(config.getExecutorConfig(), manager.getExecutorConfig());

        assertEquals(config.getServers(), manager.getServers());

        assertEquals(config.getSeeds(), manager.getSeeds());

        assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(cassandraConfigPath)),
                config.getCassandraConfig());

        assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(executorConfigPath)),
                config.getExecutorConfig());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(serversPath)).intValue(),
                config.getServers());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(seedsPath)).intValue(),
                config.getSeeds());


    }

    @Test
    public void applyConfigUpdate() throws Exception {

        ConfigurationManager manager = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();

        assertEquals(config.getCassandraConfig(), manager.getCassandraConfig());

        assertEquals(config.getExecutorConfig(), manager.getExecutorConfig());

        assertEquals(config.getServers(), manager.getServers());

        assertEquals(config.getSeeds(), manager.getSeeds());

        assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(cassandraConfigPath)),
                config.getCassandraConfig());

        assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(executorConfigPath)),
                config.getExecutorConfig());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(serversPath)).intValue(),
                config.getServers());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(seedsPath)).intValue(),
                config.getSeeds());

        manager.stop();

        CassandraConfig updatedConfig =
                config.getCassandraConfig().mutable().setJmxPort(8000)
                        .setCpus(0.6).setMemoryMb(10000).build();

        int updatedServers = config.getServers() + 10;

        int updatedSeeds = config.getSeeds() + 5;

        manager = new ConfigurationManager(
                updatedConfig,
                config.getExecutorConfig(),
                updatedServers,
                updatedSeeds,
                true,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();


        assertEquals(updatedConfig, manager.getCassandraConfig());

        assertEquals(config.getExecutorConfig(), manager.getExecutorConfig());

        assertEquals(updatedServers, manager.getServers());

        assertEquals(updatedSeeds, manager.getSeeds());

        assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(cassandraConfigPath)),
                updatedConfig);

        assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(executorConfigPath)),
                config.getExecutorConfig());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(serversPath)).intValue(),
                updatedServers);

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(seedsPath)).intValue(),
                updatedSeeds);


    }

    @Test(expected = IllegalStateException.class)
    public void failOnBadServersCount() throws Exception {

        ConfigurationManager manager = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();

        assertEquals(config.getCassandraConfig(), manager.getCassandraConfig());

        assertEquals(config.getExecutorConfig(), manager.getExecutorConfig());

        assertEquals(config.getServers(), manager.getServers());

        assertEquals(config.getSeeds(), manager.getSeeds());

        assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(cassandraConfigPath)),
                config.getCassandraConfig());

        assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(executorConfigPath)),
                config.getExecutorConfig());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(serversPath)).intValue(),
                config.getServers());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(seedsPath)).intValue(),
                config.getSeeds());

        manager.stop();


        manager = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers() - 1,
                config.getSeeds(),
                true,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();

    }

    @Test(expected = IllegalStateException.class)
    public void failOnBadSeedsCount() throws Exception {

        ConfigurationManager manager = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();

        assertEquals(config.getCassandraConfig(), manager.getCassandraConfig());

        assertEquals(config.getExecutorConfig(), manager.getExecutorConfig());

        assertEquals(config.getServers(), manager.getServers());

        assertEquals(config.getSeeds(), manager.getSeeds());

        assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(cassandraConfigPath)),
                config.getCassandraConfig());

        assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(executorConfigPath)),
                config.getExecutorConfig());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(serversPath)).intValue(),
                config.getServers());

        assertEquals(IntegerStringSerializer.get().deserialize(
                curator.getData().forPath(seedsPath)).intValue(),
                config.getSeeds());

        manager.stop();


        manager = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getServers() + 1,
                true,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        manager.start();

    }


    @AfterClass
    public static void afterAll() throws Exception {

        persistence.stop();

        server.close();

        server.stop();
    }

    @After
    public void afterEach() {

        try {
            curator.delete().forPath(cassandraConfigPath);
            curator.delete().forPath(executorConfigPath);
            curator.delete().forPath(serversPath);
            curator.delete().forPath(seedsPath);
        } catch (Exception e) {

        }
    }
}
