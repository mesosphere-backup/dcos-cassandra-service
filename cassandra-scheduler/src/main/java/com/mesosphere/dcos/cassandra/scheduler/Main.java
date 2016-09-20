package com.mesosphere.dcos.cassandra.scheduler;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.MutableSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.scheduler.health.ReconciledCheck;
import com.mesosphere.dcos.cassandra.scheduler.health.RegisteredCheck;
import com.mesosphere.dcos.cassandra.scheduler.health.ServersCheck;
import com.mesosphere.dcos.cassandra.scheduler.resources.*;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableLookup;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.mesos.scheduler.plan.api.PlanResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main extends Application<MutableSchedulerConfiguration> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    new Main().run(args);
  }

  public Main() {
    super();
  }

  @Override
  public String getName() {
    return "DCOS Cassandra Service";
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

  private void registerJerseyResources(Environment environment, Injector injector) {
    environment.jersey().register(
      injector.getInstance(ServiceConfigResource.class));
    environment.jersey().register(
      injector.getInstance(SeedsResource.class));
    environment.jersey().register(
      injector.getInstance(ConfigurationResource.class));
    environment.jersey().register(
      injector.getInstance(TasksResource.class));
    environment.jersey().register(
      injector.getInstance(BackupResource.class));
    environment.jersey().register(
      injector.getInstance(PlanResource.class));
    environment.jersey().register(
      injector.getInstance(RestoreResource.class));
    environment.jersey().register(
      injector.getInstance(CleanupResource.class));
    environment.jersey().register(
      injector.getInstance(RepairResource.class));
    environment.jersey().register(
      injector.getInstance(DataCenterResource.class)
    );
    environment.jersey().register(
            injector.getInstance(ConnectionResource.class)
    );
  }

  private void registerManagedObjects(Environment environment, Injector injector) {
    environment.lifecycle().manage(
      injector.getInstance(ConfigurationManager.class));
    environment.lifecycle().manage(
      injector.getInstance(CassandraTasks.class));
    environment.lifecycle().manage(
      injector.getInstance(CassandraScheduler.class));
  }

  private void registerHealthChecks(Environment environment,
                                    Injector injector) {
    environment.healthChecks().register(RegisteredCheck.NAME,
      injector.getInstance(RegisteredCheck.class));
    environment.healthChecks().register(ServersCheck.NAME,
      injector.getInstance(ServersCheck.class));
    environment.healthChecks().register(ReconciledCheck.NAME,
      injector.getInstance(ReconciledCheck.class));
  }


  private void logConfiguration(MutableSchedulerConfiguration config) {

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
