package com.mesosphere.dcos.cassandra.executor.tasks;

import org.apache.mesos.executor.TimedExecutorTask;

import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates Tasks which set Ulimits
 */
public class UlimitExecutorTask implements TimedExecutorTask {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final String arg;
    private final String value;
    private final ProcessBuilder processBuilder;
    private final Duration timeout;
    private Process process;

    public UlimitExecutorTask(String arg, String value) {
        this.arg = arg;
        this.value = value;
        this.processBuilder = new ProcessBuilder("ulimit", arg, value);
        this.timeout = Duration.ofSeconds(30);
    }

    @Override
    public Duration getTimeout() {
        return timeout;
    }

    @Override
    public void stop() {
        if (process != null) {
            process.destroyForcibly();
        }
    }

    @Override
    public void run() {
        try {
            process = processBuilder.start();
            process.wait(timeout.toMillis());
            if (process.exitValue() != 0) {
                LOGGER.error("Failed to run command: '{}' with exit code: '{}'",
                        processBuilder.command(), process.exitValue());
                stop();
            } else {
                LOGGER.info("Successfully ran command: '{}'", processBuilder.command());
            }
        } catch (InterruptedException | IOException e) {
            String errMsg = String.format("Failed to run command: '{}' with exception: ", processBuilder.command());
            LOGGER.error(errMsg, e);
            stop();
        }
    }
}
