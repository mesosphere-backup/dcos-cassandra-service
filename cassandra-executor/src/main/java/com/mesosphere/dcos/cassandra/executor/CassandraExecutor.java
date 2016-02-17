package com.mesosphere.dcos.cassandra.executor;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class CassandraExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraExecutor.class
    );


    private ExecutorDriver driver;
    private volatile CassandraDaemonProcess cassandra;
    private final ScheduledExecutorService executor;

    @Inject
    public CassandraExecutor(final ScheduledExecutorService executor) {
        this.executor = executor;
    }


    @Override
    public void registered(ExecutorDriver driver,
                           Protos.ExecutorInfo executorInfo,
                           Protos.FrameworkInfo frameworkInfo,
                           Protos.SlaveInfo slaveInfo) {

        this.driver = driver;
    }

    @Override
    public void reregistered(ExecutorDriver driver,
                             Protos.SlaveInfo slaveInfo) {

        this.driver = driver;

    }

    @Override
    public void disconnected(ExecutorDriver driver) {

    }

    @Override
    public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {

        try {
            CassandraTask cassandraTask = CassandraTask.parse(task);

            switch (cassandraTask.getType()) {

                case CASSANDRA_DAEMON:
                    if (cassandra != null && cassandra.isOpen()) {
                        Protos.TaskStatus daemonStatus = CassandraDaemonStatus
                                .create(
                                        Protos.TaskState.TASK_FAILED,
                                        cassandraTask.getId(),
                                        cassandraTask.getSlaveId(),
                                        cassandraTask.getExecutor().getId(),
                                        Optional.of(
                                                "Cassandra Daemon is already running"),
                                        CassandraMode.DRAINED).toProto();

                        driver.sendStatusUpdate(daemonStatus);

                        LOGGER.error("CassandraDaemon task failed status = {}",
                                daemonStatus);
                    } else {
                        cassandra = CassandraDaemonProcess.create(
                                (CassandraDaemonTask) cassandraTask,
                                executor,
                                driver
                        );

                        LOGGER.info("Starting CassandraDaemon task = {}",
                                cassandraTask);
                    }
                    break;


                case S3_BACKUP:
                    Protos.TaskStatus backupStatus = S3BackupStatus.create(
                            Protos.TaskState.TASK_FAILED,
                            cassandraTask.getId(),
                            cassandraTask.getSlaveId(),
                            cassandraTask.getExecutor().getId(),
                            Optional.of("Not Implemented")
                    ).toProto();
                    driver.sendStatusUpdate(backupStatus);
                    LOGGER.error("S3Backup task failed status = {}",
                            backupStatus);

                    break;

                case S3_RESTORE:
                    Protos.TaskStatus restoreStatus = S3BackupStatus.create(
                            Protos.TaskState.TASK_FAILED,
                            cassandraTask.getId(),
                            cassandraTask.getSlaveId(),
                            cassandraTask.getExecutor().getId(),
                            Optional.of("Not Implemented")
                    ).toProto();
                    driver.sendStatusUpdate(restoreStatus);
                    LOGGER.error("S3Backup task failed status = {}",
                            restoreStatus);

                    break;

            }

        } catch (Throwable t) {

            LOGGER.error(String.format("Error launching task = %s", task), t);

            Protos.TaskStatus failed = Protos.TaskStatus
                    .newBuilder()
                    .setState(Protos.TaskState.TASK_FAILED)
                    .setSlaveId(task.getSlaveId())
                    .setExecutorId(task.getExecutor().getExecutorId())
                    .setTaskId(task.getTaskId())
                    .setMessage(String.format("Exception launching task %s",
                            t.getMessage())).build();

            driver.sendStatusUpdate(failed);

            LOGGER.error("Sent failed status update = {}", failed);
        }

    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {

        if (cassandra != null && cassandra.isOpen()) {

            if (taskId.equals(cassandra.getTask().getId())) {
                LOGGER.info("Killing CassandraDaemon");
                cassandra.kill();

            } else {
                LOGGER.info("Unknown TaskId = ${}", taskId);
            }
        } else {
            LOGGER.error("CassandraDaemon is not running");
        }

    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {

        LOGGER.error("You should not be using unreliable messaging buddy!");

    }

    @Override
    public void shutdown(ExecutorDriver driver) {

        LOGGER.info("Shutting down now");
        if (cassandra != null && cassandra.isOpen()) {
            LOGGER.info("Disconnected Killing CassandraDaemon");
            cassandra.kill();
        }

        executor.shutdownNow();
    }

    @Override
    public void error(ExecutorDriver driver, String message) {

        LOGGER.error("Error {}", message);

    }

    public Optional<CassandraDaemonProcess> getCassandraDaemon() {
        return (cassandra != null && cassandra.isOpen()) ?
                Optional.of(cassandra) : Optional.empty();
    }
}
