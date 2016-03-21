package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupStatus;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupTask;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


public class Cleanup implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cleanup.class);

    private final NodeProbe probe;
    private final ExecutorDriver driver;
    private final CleanupTask task;

    public Cleanup(final ExecutorDriver driver,
                   final NodeProbe probe,
                   final CleanupTask task) {
        this.driver = driver;
        this.probe = probe;
        this.task = task;
    }

    private void globalCleanup()
            throws InterruptedException, ExecutionException, IOException {
        LOGGER.info("Performing global cleanup");
        probe.forceKeyspaceCleanup(null);
    }

    private List<String> getKeySpaces() {
        if (task.getKeySpaces().isEmpty()) {
            return probe.getNonSystemKeyspaces();
        } else {
            return task.getKeySpaces();
        }
    }

    private String[] getColumnFamilies() {
        if (task.getColumnFamilies().isEmpty()) {
            return new String[0];
        } else {
            String[] cf = new String[task.getColumnFamilies().size()];
            return task.getColumnFamilies().toArray(cf);
        }
    }

    private void cleanupKeySpace(String keySpace, String[] columnFamiles)
            throws InterruptedException, ExecutionException, IOException {
        LOGGER.info("Performing cleanup of selected columnFamilies in {}",
                keySpace);
        probe.forceKeyspaceCleanup(keySpace, columnFamiles);
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING

            final List<String> keySpaces = getKeySpaces();
            final String[] columnFamilies = getColumnFamilies();
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    String.format("Starting cleanup: keySpaces = %s, " +
                                    "columnFamilies = %s",
                            keySpaces,
                            Arrays.asList(columnFamilies)));

            for (String keyspace : keySpaces) {
                LOGGER.info("Starting cleanup : keySpace = {}, " +
                                "columnFamilies = {}",
                        keyspace,
                        Arrays.asList(columnFamilies));

                probe.forceKeyspaceCleanup(keyspace, columnFamilies);

                LOGGER.info("Completed cleanup : keySpace = {}, " +
                                "columnFamilies = {}",
                        keyspace,
                        Arrays.asList(columnFamilies));
            }


            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    String.format("Completed cleanup: keySpaces = %s, " +
                                    "columnFamilies = %s",
                            keySpaces,
                            Arrays.asList(columnFamilies)));
        } catch (Exception e) {
            // Send TASK_FAILED
            final String errorMessage = "Cleanup Failed Reason: " + e;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        }
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state, String message) {
        Protos.TaskStatus status = CleanupStatus.create(
                state,
                task.getId(),
                task.getSlaveId(),
                task.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }
}
