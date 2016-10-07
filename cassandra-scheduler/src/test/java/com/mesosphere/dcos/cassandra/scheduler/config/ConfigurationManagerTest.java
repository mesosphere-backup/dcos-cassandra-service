package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;

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
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.state.StateStore;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class ConfigurationManagerTest {

  private static TestingServer server;

  private static CuratorFramework curator;

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
                "/" + original.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        DefaultConfigurationManager configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + original.getServiceConfig().getName(),
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
                "/jre/location",
                "/executor/location",
                "/cassandra/location",
                "");
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

        configurationManager
                = new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                "/" + original.getServiceConfig().getName(),
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

    curator = persistence.getCurator();

    cassandraConfigPath = "/cassandra/" + config.getIdentity().getName() +
      "/cassandraConfig";

    executorConfigPath = "/cassandra/" + config.getIdentity().getName()
      + "/executorConfig";

    serversPath = "/cassandra/" + config.getIdentity().getName() +
      "/servers";

    seedsPath = "/cassandra/" + config.getIdentity().getName() + "/seeds";

  }

  @Test
  public void loadAndPersistConfiguration() throws Exception {

    ConfigurationManager manager = new ConfigurationManager(
      config.getCassandraConfig(),
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()

    );

    assertEquals("http://datastax.marathon.mesos:8080/v1/seeds", config.getSeedsUrl());
    assertEquals("cassandra", config.getIdentity().getName());
    assertEquals("dse-role", config.getIdentity().getRole());
    assertEquals("dse-cluster", config.getIdentity().getCluster());
    assertEquals("dse-principal",
      config.getIdentity().getPrincipal());
    assertEquals("", config.getIdentity().getSecret());

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
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
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

    CassandraConfig updatedCassandraConfig =
      config.getCassandraConfig().mutable().setJmxPort(8000)
        .setCpus(0.6).setMemoryMb(10000).build();

    ExecutorConfig updatedExecutorConfig = new ExecutorConfig(
      "/command/line",
      new ArrayList<>(),
      1.2,
      345,
      901,
      17,
      "/java/home",
      "/jre/location",
      "/executor/location",
      "/cassandra/location",
      "",
      "unlimited",
      "100000",
      "32768");

    int updatedServers = config.getServers() + 10;

    int updatedSeeds = config.getSeeds() + 5;

    manager = new ConfigurationManager(
      updatedCassandraConfig,
      config.getClusterTaskConfig(),
      updatedExecutorConfig,
      updatedServers,
      updatedSeeds,
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()
    );

    manager.start();


    assertEquals(updatedCassandraConfig, manager.getCassandraConfig());

    assertEquals(updatedExecutorConfig, manager.getExecutorConfig());

    assertEquals(updatedServers, manager.getServers());

    assertEquals(updatedSeeds, manager.getSeeds());

    assertEquals(CassandraConfig.JSON_SERIALIZER.deserialize(
      curator.getData().forPath(cassandraConfigPath)),
      updatedCassandraConfig);

    assertEquals(ExecutorConfig.JSON_SERIALIZER.deserialize(
      curator.getData().forPath(executorConfigPath)),
      updatedExecutorConfig);

    assertEquals(IntegerStringSerializer.get().deserialize(
      curator.getData().forPath(serversPath)).intValue(),
      updatedServers);

    assertEquals(IntegerStringSerializer.get().deserialize(
      curator.getData().forPath(seedsPath)).intValue(),
      updatedSeeds);


  }

  public void failOnBadServersCount() throws Exception {

    ConfigurationManager manager = new ConfigurationManager(
      config.getCassandraConfig(),
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
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
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers() - 1,
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()
    );

    manager.start();

    assertEquals(manager.getErrors().size(), 1);

  }


  public void failOnBadSeedsCount() throws Exception {

    ConfigurationManager manager = new ConfigurationManager(
      config.getCassandraConfig(),
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
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
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getServers() + 1,
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()
    );

    manager.start();
    assertEquals(manager.getErrors().size(), 1);

  }

  @Test
  public void testSetServers() throws Exception {

    ConfigurationManager manager = new ConfigurationManager(
      config.getCassandraConfig(),
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()
    );

    manager.start();

    assertNotSame(17, manager.getServers());
    manager.setServers(17);
    assertEquals(17, manager.getServers());
    assertEquals(17, IntegerStringSerializer.get().deserialize(
      curator.getData().forPath(serversPath)).intValue());

  }

  @Test
  public void testSetSeeds() throws Exception {

    ConfigurationManager manager = new ConfigurationManager(
      config.getCassandraConfig(),
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()
    );

    manager.start();

    assertNotSame(29, manager.getSeeds());
    manager.setSeeds(29);
    assertEquals(29, manager.getSeeds());
    assertEquals(29, IntegerStringSerializer.get().deserialize(
      curator.getData().forPath(seedsPath)).intValue());

  }

  @Test
  public void testSetCassandraConfig() throws Exception {

    ConfigurationManager manager = new ConfigurationManager(
      config.getCassandraConfig(),
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()
    );

    manager.start();

    CassandraConfig new_config = config.getCassandraConfig().mutable().setJmxPort(13000)
      .setCpus(2.3).setMemoryMb(1234).build();

    assertNotSame(new_config, manager.getCassandraConfig());
    manager.setCassandraConfig(new_config);
    assertEquals(new_config, manager.getCassandraConfig());
    assertEquals(new_config, CassandraConfig.JSON_SERIALIZER.deserialize(
      curator.getData().forPath(cassandraConfigPath)));

  }

  @Test
  public void testSetExecutorConfig() throws Exception {

    ConfigurationManager manager = new ConfigurationManager(
      config.getCassandraConfig(),
      config.getClusterTaskConfig(),
      config.getExecutorConfig(),
      config.getServers(),
      config.getSeeds(),
      "NODE",
      config.getSeedsUrl(),
      config.getDcUrl(),
      config.getExternalDcsList(),
      config.getExternalDcSyncMs(),
      persistence,
      CassandraConfig.JSON_SERIALIZER,
      ExecutorConfig.JSON_SERIALIZER,
      ClusterTaskConfig.JSON_SERIALIZER,
      IntegerStringSerializer.get()
    );

    manager.start();

    ExecutorConfig new_config = new ExecutorConfig(
      "/command/line",
      new ArrayList<>(),
      1.2,
      345,
      901,
      17,
      "/java/home",
      "/jre/location",
      "/executor/location",
      "/cassandra/location",
      "",
      "unlimited",
      "100000",
      "32768");

    assertNotSame(new_config, manager.getExecutorConfig());
    manager.setExecutorConfig(new_config);
    assertEquals(new_config, manager.getExecutorConfig());
    assertEquals(new_config, ExecutorConfig.JSON_SERIALIZER.deserialize(
      curator.getData().forPath(executorConfigPath)));
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
