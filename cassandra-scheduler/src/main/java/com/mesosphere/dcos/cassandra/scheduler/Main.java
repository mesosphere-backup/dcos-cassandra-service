package com.mesosphere.dcos.cassandra.scheduler;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.config.MutableSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.health.RegisteredCheck;
import com.mesosphere.dcos.cassandra.scheduler.health.ServersCheck;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableLookup;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.mesos.scheduler.SchedulerErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main extends Application<MutableSchedulerConfiguration> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    try {
      new Main().run(args);
    } catch (Exception e) {
      LOGGER.error("Caught exception while trying to run main: exiting framework process");
      System.exit(SchedulerErrorCode.ERROR.ordinal());
    }
  }

  public Main() {
    super();
  }

  @Override
  public String getName() {
    return "DC/OS Cassandra Service";
  }

  @Override
  public void initialize(Bootstrap<MutableSchedulerConfiguration> bootstrap) {
    super.initialize(bootstrap);

    StrSubstitutor strSubstitutor = new StrSubstitutor(new EnvironmentVariableLookup(false));
    strSubstitutor.setEnableSubstitutionInVariables(true);

    bootstrap.addBundle(new Java8Bundle());
    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(
        bootstrap.getConfigurationSourceProvider(),
        strSubstitutor));
  }

  @Override
  public void run(MutableSchedulerConfiguration configuration,
                  Environment environment) throws Exception {
    logConfiguration(configuration);

    final SchedulerModule baseModule = new SchedulerModule(
      configuration.createConfig(),
      configuration.getCuratorConfig(),
      configuration.getMesosConfig(),
      environment);

    Injector injector = Guice.createInjector(baseModule);

    registerManagedObjects(environment, injector);
    registerJerseyResources(environment, injector);
    registerHealthChecks(environment, injector);
  }

  private void registerJerseyResources(Environment environment, Injector injector) throws Exception {
    CassandraScheduler scheduler = injector.getInstance(CassandraScheduler.class);
    scheduler.registerFramework();
    for (Object o : scheduler.getResources()) {
      environment.jersey().register(o);
    }
  }

  private void registerManagedObjects(Environment environment, Injector injector) {
    environment.lifecycle().manage(
      injector.getInstance(ConfigurationManager.class));
    environment.lifecycle().manage(
      injector.getInstance(CassandraState.class));
  }

  private void registerHealthChecks(Environment environment,
                                    Injector injector) {
    environment.healthChecks().register(RegisteredCheck.NAME,
      injector.getInstance(RegisteredCheck.class));
    environment.healthChecks().register(ServersCheck.NAME,
      injector.getInstance(ServersCheck.class));
  }


  private void logConfiguration(MutableSchedulerConfiguration config) {
    LOGGER.info("Full configuration: {}", config);

    LOGGER.info("Framework ServiceConfig = {}",
      config.getServiceConfig());
    LOGGER.info("Framework Mesos Configuration = {}",
      config.getMesosConfig());
    LOGGER.info("Framework ZooKeeper Configuration = {}",
      config.getCuratorConfig());
    LOGGER.info(
      "------------ Cassandra Configuration ------------");
    LOGGER.info("heap = {}", config.getCassandraConfig().getHeap());
    LOGGER.info("jmx port = {}", config.getCassandraConfig()
      .getJmxPort());
    LOGGER.info("location = {}", config.getCassandraConfig()
      .getLocation());
    config
      .getCassandraConfig()
      .getApplication()
      .toMap()
      .forEach((key, value) -> LOGGER.info("{} = {}", key, value));
  }

}
