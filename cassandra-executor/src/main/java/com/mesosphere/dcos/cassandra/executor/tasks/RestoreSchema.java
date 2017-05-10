package com.mesosphere.dcos.cassandra.executor.tasks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.Future;

/**
 * Restores the schema first before restoring the data and only if it does not exist.
 */
public class RestoreSchema implements ExecutorTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSchema.class);

    private final ExecutorDriver driver;
    private final BackupRestoreContext context;
    private CassandraDaemonProcess daemon;
    private final RestoreSchemaTask cassandraTask;
    private BackupStorageDriver backupStorageDriver;

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {
        final Protos.TaskStatus status =
                cassandraTask.createStatus(state,Optional.of(message)).getTaskStatus();
        driver.sendStatusUpdate(status);
    }

    /**
     * Constructs the RestoreSchema
     * @param driver The ExecutorDriver used to send task status.
     * @param daemon The CassandraDaemonProcess on which to restore schema.
     * @param cassandraTask The CassandraTask that will be executed by the restore schema.
     * @param backupStorageDriver To download schema, for restoring.
     */
    public RestoreSchema(ExecutorDriver driver,
                         CassandraDaemonProcess daemon,
                         RestoreSchemaTask cassandraTask,
                         BackupStorageDriver backupStorageDriver) {
        this.driver = driver;
        this.cassandraTask = cassandraTask;
        this.daemon = daemon;
        this.backupStorageDriver = backupStorageDriver;
        context = cassandraTask.getBackupRestoreContext();
    }

    @Override
    public void run() {
        Cluster cluster = null;
        Session session = null;
        Scanner read = null;
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started restoring schema");

            //cluster = Cluster.builder().addContactPoint(daemon.getProbe().getEndpoint()).build();
            cluster = Cluster.builder().addContactPoint(daemon.getProbe().getEndpoint()).withCredentials(context.getUsername(), context.getPassword()).build();
            session = cluster.connect();
            read = new Scanner(backupStorageDriver.downloadSchema(context));
            read.useDelimiter(";");
            while (read.hasNext()) {
                try {
                    String cqlStmt = read.next().trim();
                    if (cqlStmt.isEmpty())
                        continue;
                    cqlStmt += ";";
                    session.execute(cqlStmt);
                } catch (AlreadyExistsException e) {
                    LOGGER.info("Schema already exists: {}", e.toString());
                }
            }

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished restoring schema");
        } catch (Throwable t) {
            // Send TASK_FAILED
            final String errorMessage = "Failed restoring schema. Reason: " + t;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        } finally {
            if (read != null)
                read.close();
            if (session != null)
                session.close();
            if (cluster != null)
                cluster.close();
        }
    }

    @Override
    public void stop(Future<?> future) {
        future.cancel(true);
    }
}
