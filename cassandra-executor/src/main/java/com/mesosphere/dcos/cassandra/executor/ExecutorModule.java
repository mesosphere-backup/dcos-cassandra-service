package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.AbstractModule;
import com.mesosphere.dcos.cassandra.executor.config.CassandraExecutorConfiguration;

/**
 * Created by kowens on 2/10/16.
 */
public class ExecutorModule extends AbstractModule {

    final CassandraExecutorConfiguration configuration;

    public static ExecutorModule create(
            final CassandraExecutorConfiguration configuration) {
        return new ExecutorModule(configuration);
    }

    public ExecutorModule(final CassandraExecutorConfiguration configuration) {

        this.configuration = configuration;

    }


    @Override
    protected void configure() {

        bind(CassandraExecutor.class).asEagerSingleton();

    }
}
