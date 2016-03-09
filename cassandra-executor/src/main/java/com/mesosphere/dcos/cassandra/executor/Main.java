package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mesosphere.dcos.cassandra.executor.checks.DaemonMode;
import com.mesosphere.dcos.cassandra.executor.checks.DaemonRunning;
import com.mesosphere.dcos.cassandra.executor.config.CassandraExecutorConfiguration;
import com.mesosphere.dcos.cassandra.executor.resources.CassandraDaemonController;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableLookup;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.mesos.MesosExecutorDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main extends Application<CassandraExecutorConfiguration> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    protected Main() {
        super();
    }

    @Override
    public String getName() {
        return "DCOS Cassandra Executor";
    }

    @Override
    public void initialize(Bootstrap<CassandraExecutorConfiguration> bootstrap) {
        super.initialize(bootstrap);

        bootstrap.addBundle(new Java8Bundle());
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(
                        bootstrap.getConfigurationSourceProvider(),
                        new StrSubstitutor(
                                new EnvironmentVariableLookup(false))));
    }

    @Override
    public void run(CassandraExecutorConfiguration configuration,
                    Environment environment) throws Exception {

        final ExecutorModule baseModule = new ExecutorModule(configuration);

        Injector injector = Guice.createInjector(baseModule);

        environment.healthChecks().register(DaemonRunning.NAME,
                injector.getInstance(DaemonRunning.class));
        environment.healthChecks().register(DaemonMode.NAME,
                injector.getInstance(DaemonMode.class));
        environment.jersey().register(
                injector.getInstance(CassandraDaemonController.class));

        environment.lifecycle().manage(
                injector.getInstance(ExecutorDriverDispatcher.class));
    }
}
