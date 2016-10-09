package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.ConfigValidationError;
import com.mesosphere.dcos.cassandra.common.config.ConfigValidator;
import com.mesosphere.dcos.cassandra.common.config.MutableSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.ServiceConfig;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

public class ConfigValidatorTest {
  ConfigurationFactory<MutableSchedulerConfiguration> factory;
  MutableSchedulerConfiguration configuration;

  @Before
  public void beforeEach() throws Exception {
    factory = new ConfigurationFactory<>(
      MutableSchedulerConfiguration.class,
      BaseValidator.newValidator(),
      Jackson.newObjectMapper().registerModule(new GuavaModule())
        .registerModule(new Jdk8Module()),
      "dw");

    configuration = factory.build(
      new SubstitutingSourceProvider(
        new FileConfigurationSourceProvider(),
        new EnvironmentVariableSubstitutor(false, true)),
      Resources.getResource("scheduler.yml").getFile());
  }

  @Test
  public void testName() throws Exception {
    MutableSchedulerConfiguration mutable = factory.build(
      new SubstitutingSourceProvider(
        new FileConfigurationSourceProvider(),
        new EnvironmentVariableSubstitutor(false, true)),
      Resources.getResource("scheduler.yml").getFile());
    mutable.getCassandraConfig().getApplication().writeDaemonConfiguration(Paths.get(".").resolve("cassandra.yaml"));
    final ServiceConfig serviceConfig = mutable.getServiceConfig();
    final ServiceConfig updated = ServiceConfig.create("yo",
      serviceConfig.getId(),
      serviceConfig.getVersion(),
      serviceConfig.getUser(),
      serviceConfig.getCluster(),
      serviceConfig.getRole(),
      serviceConfig.getPrincipal(),
      serviceConfig.getFailoverTimeoutS(),
      serviceConfig.getSecret(),
      serviceConfig.isCheckpoint());
    mutable.setServiceConfig(updated);
    final ConfigValidator configValidator = new ConfigValidator();
    final List<ConfigValidationError> validate = configValidator.validate(
      configuration.createConfig(),
      mutable.createConfig());
    Assert.assertTrue(validate.size() == 1);
  }

  @Test
  public void testCluster() throws Exception {
    MutableSchedulerConfiguration mutable = factory.build(
      new SubstitutingSourceProvider(
        new FileConfigurationSourceProvider(),
        new EnvironmentVariableSubstitutor(false, true)),
      Resources.getResource("scheduler.yml").getFile());
    final ServiceConfig serviceConfig = mutable.getServiceConfig();
    final ServiceConfig updated = ServiceConfig.create(serviceConfig.getName(),
      serviceConfig.getId(),
      serviceConfig.getVersion(),
      serviceConfig.getUser(),
      serviceConfig.getCluster() + "1234",
      serviceConfig.getRole(),
      serviceConfig.getPrincipal(),
      serviceConfig.getFailoverTimeoutS(),
      serviceConfig.getSecret(),
      serviceConfig.isCheckpoint());
    mutable.setServiceConfig(updated);
    final ConfigValidator configValidator = new ConfigValidator();
    final List<ConfigValidationError> validate = configValidator.validate(configuration.createConfig(), mutable.createConfig());
    Assert.assertTrue(validate.size() == 1);
  }

  @Test
  public void testPrincipal() throws Exception {
    MutableSchedulerConfiguration mutable = factory.build(
      new SubstitutingSourceProvider(
        new FileConfigurationSourceProvider(),
        new EnvironmentVariableSubstitutor(false, true)),
      Resources.getResource("scheduler.yml").getFile());
    final ServiceConfig serviceConfig = mutable.getServiceConfig();
    final ServiceConfig updated = ServiceConfig.create(serviceConfig.getName(),
      serviceConfig.getId(),
      serviceConfig.getVersion(),
      serviceConfig.getUser(),
      serviceConfig.getCluster(),
      serviceConfig.getRole(),
      serviceConfig.getPrincipal() + "asdf",
      serviceConfig.getFailoverTimeoutS(),
      serviceConfig.getSecret(),
      serviceConfig.isCheckpoint());
    mutable.setServiceConfig(updated);
    final ConfigValidator configValidator = new ConfigValidator();
    final List<ConfigValidationError> validate =
      configValidator.validate(configuration.createConfig(), mutable.createConfig());
    Assert.assertTrue(validate.size() == 1);
  }

  @Test
  public void testRole() throws Exception {

      MutableSchedulerConfiguration mutable = factory.build(
        new SubstitutingSourceProvider(
          new FileConfigurationSourceProvider(),
          new EnvironmentVariableSubstitutor(false, true)),
        Resources.getResource("scheduler.yml").getFile());
    final ServiceConfig serviceConfig = mutable.getServiceConfig();
    final ServiceConfig updated = ServiceConfig.create(serviceConfig.getName(),
      serviceConfig.getId(),
      serviceConfig.getVersion(),
      serviceConfig.getUser(),
      serviceConfig.getCluster(),
      serviceConfig.getRole() + "qwerty",
      serviceConfig.getPrincipal(),
      serviceConfig.getFailoverTimeoutS(),
      serviceConfig.getSecret(),
      serviceConfig.isCheckpoint());
    mutable.setServiceConfig(updated);
    final ConfigValidator configValidator = new ConfigValidator();
    final List<ConfigValidationError> validate = configValidator.validate(configuration.createConfig(),mutable.createConfig());
    Assert.assertTrue(validate.size() == 1);
  }
}
