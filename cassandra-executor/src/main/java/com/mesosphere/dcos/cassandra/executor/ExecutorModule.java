package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.AbstractModule;
import com.mesosphere.dcos.cassandra.executor.config.CassandraExecutorConfiguration;
import org.apache.mesos.Executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
