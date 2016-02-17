package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class ExecutorDriverDispatcher implements Runnable, Managed{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ExecutorDriverDispatcher.class);

    private static final int exitCode(final Protos.Status status){
        return (status == Protos.Status.DRIVER_ABORTED ||
                status == Protos.Status.DRIVER_NOT_STARTED) ?
                -1 : 0;
    }

    private final ExecutorDriver driver;
    private final ExecutorService executor;

    @Inject
    public ExecutorDriverDispatcher(final ExecutorDriverFactory driverFactory,
                                    final Executor executor,
                                    final ExecutorService executorService) {
        this.driver = driverFactory.getDriver(executor);
        this.executor = executorService;
    }

    @Override
    public void run() {

        LOGGER.info("Starting driver execution");
        Protos.Status status = driver.run();
        LOGGER.info("Driver execution complete status = {}",status);
        driver.stop();
        System.exit(exitCode(status));
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting ExecutorDriver");
        executor.submit(this);
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping ExecutorDriver");
        this.driver.stop();
    }
}
