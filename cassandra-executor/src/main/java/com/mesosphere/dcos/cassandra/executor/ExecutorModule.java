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

import com.google.inject.AbstractModule;
import com.mesosphere.dcos.cassandra.executor.config.CassandraExecutorConfiguration;
import org.apache.mesos.Executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The dependency injection module for the Cassandra executor.
 */
public class ExecutorModule extends AbstractModule {

    final CassandraExecutorConfiguration configuration;

    /**
     * Creates a new ExecutorModule given the application configuration.
     * @param configuration The application Configuration used to inject
     *                      dependencies.
     * @return The ExecutorModule constructed using configuration.
     */
    public static ExecutorModule create(
            final CassandraExecutorConfiguration configuration) {
        return new ExecutorModule(configuration);
    }

    /**
     * Constructs a new ExecutorModule given the application configuration.
     * @param configuration The application Configuration used to inject
     *                      dependencies.
     */
    public ExecutorModule(final CassandraExecutorConfiguration configuration) {
        this.configuration = configuration;
    }


    @Override
    protected void configure() {

        bind(ExecutorService.class).toInstance(
                Executors.newCachedThreadPool());
        bind(ScheduledExecutorService.class).toInstance(
                Executors.newScheduledThreadPool(10));
        bind(Executor.class).to(CassandraExecutor.class).asEagerSingleton();
        bind(ExecutorDriverFactory.class)
                .to(MesosExecutorDriverFactory.class)
                .asEagerSingleton();

    }
}
