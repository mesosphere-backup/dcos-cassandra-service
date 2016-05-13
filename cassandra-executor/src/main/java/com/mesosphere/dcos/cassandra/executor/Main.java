/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for the Cassandra executor program.
 */
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
