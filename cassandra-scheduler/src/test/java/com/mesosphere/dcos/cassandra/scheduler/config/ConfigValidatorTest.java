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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ConfigValidatorTest {
    ConfigurationFactory<DropwizardConfiguration> factory;
    CassandraSchedulerConfiguration configuration;

    @Before
    public void beforeEach() throws Exception {
        factory = new ConfigurationFactory<>(
                        DropwizardConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");

        configuration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile()).getSchedulerConfiguration();
    }

    @Test
    public void testName() throws Exception {
        CassandraSchedulerConfiguration newConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile()).getSchedulerConfiguration();
        final Identity identity = newConfiguration.getIdentity();
        final Identity updated = Identity.create("yo",
                identity.getId(),
                identity.getVersion(),
                identity.getUser(),
                identity.getCluster(),
                identity.getRole(),
                identity.getPrincipal(),
                identity.getFailoverTimeoutS(),
                identity.getSecret(),
                identity.isCheckpoint());
        newConfiguration.setIdentity(updated);
        final ConfigValidator configValidator = new ConfigValidator();
        final List<ConfigValidationError> validate = configValidator.validate(configuration, newConfiguration);
        Assert.assertTrue(validate.size() == 1);
    }

    @Test
    public void testCluster() throws Exception {
        CassandraSchedulerConfiguration newConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile()).getSchedulerConfiguration();
        final Identity identity = newConfiguration.getIdentity();
        final Identity updated = Identity.create(identity.getName(),
                identity.getId(),
                identity.getVersion(),
                identity.getUser(),
                identity.getCluster() + "1234",
                identity.getRole(),
                identity.getPrincipal(),
                identity.getFailoverTimeoutS(),
                identity.getSecret(),
                identity.isCheckpoint());
        newConfiguration.setIdentity(updated);
        final ConfigValidator configValidator = new ConfigValidator();
        final List<ConfigValidationError> validate = configValidator.validate(configuration, newConfiguration);
        Assert.assertTrue(validate.size() == 1);
    }

    @Test
    public void testPrincipal() throws Exception {
        CassandraSchedulerConfiguration newConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile()).getSchedulerConfiguration();
        final Identity identity = newConfiguration.getIdentity();
        final Identity updated = Identity.create(identity.getName(),
                identity.getId(),
                identity.getVersion(),
                identity.getUser(),
                identity.getCluster(),
                identity.getRole(),
                identity.getPrincipal() + "asdf",
                identity.getFailoverTimeoutS(),
                identity.getSecret(),
                identity.isCheckpoint());
        newConfiguration.setIdentity(updated);
        final ConfigValidator configValidator = new ConfigValidator();
        final List<ConfigValidationError> validate = configValidator.validate(configuration, newConfiguration);
        Assert.assertTrue(validate.size() == 1);
    }

    @Test
    public void testRole() throws Exception {
        CassandraSchedulerConfiguration newConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile()).getSchedulerConfiguration();
        final Identity identity = newConfiguration.getIdentity();
        final Identity updated = Identity.create(identity.getName(),
                identity.getId(),
                identity.getVersion(),
                identity.getUser(),
                identity.getCluster(),
                identity.getRole() + "qwerty",
                identity.getPrincipal(),
                identity.getFailoverTimeoutS(),
                identity.getSecret(),
                identity.isCheckpoint());
        newConfiguration.setIdentity(updated);
        final ConfigValidator configValidator = new ConfigValidator();
        final List<ConfigValidationError> validate = configValidator.validate(configuration, newConfiguration);
        Assert.assertTrue(validate.size() == 1);
    }
}
