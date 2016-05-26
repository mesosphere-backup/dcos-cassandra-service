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


import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonStatus;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import com.mesosphere.dcos.cassandra.executor.metrics.MetricsConfig;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The CassandraDaemonProcess launches the Cassandra process process,
 * monitors its current mode and status, and reports changes to the scheduler
 * . If the Cassandra daemon terminates the CassandraDaemonProcess causes the
 * executor to exit.
 * All administration and monitoring is achieved by attaching to the Cassandra
 * daemon via JMX using the NodeProbe class.
 */
public class CassandraDaemonProcess {


    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraDaemonProcess.class);

    private static final Object CLOSED = new Object();

    private static class WatchDog implements Runnable {

        public static final WatchDog create(
                final CassandraDaemonTask task,
                final Process process,
                final ExecutorDriver driver,
                final AtomicBoolean open,
                final CompletableFuture<Object> closeFuture) {

            return new WatchDog(task, process, driver, open, closeFuture);
        }

        private final CassandraDaemonTask task;
        private final ExecutorDriver driver;
        private final Process process;
        private final AtomicBoolean open;
        private final CompletableFuture<Object> closeFuture;

        public WatchDog(final CassandraDaemonTask task,
                        final Process process,
                        final ExecutorDriver driver,
                        final AtomicBoolean open,
                        final CompletableFuture<Object> closeFuture) {

            this.task = task;
            this.driver = driver;
            this.process = process;
            this.open = open;
            this.closeFuture = closeFuture;

        }

        public void run() {

            while (true) {

                try {

                    process.waitFor();
                    int exitCode = process.exitValue();
                    LOGGER.info("Cassandra Daemon terminated: exit code = {}",
                            exitCode);
                    Protos.TaskState state;
                    String message;

                    if (exitCode == 0) {
                        state = Protos.TaskState.TASK_FINISHED;
                        message = "Cassandra Daemon exited normally";
                    } else if (exitCode > 128) {
                        state = Protos.TaskState.TASK_KILLED;
                        message = "Cassandra Daemon was killed by signal " +
                                (exitCode - 128);
                    } else {
                        state = Protos.TaskState.TASK_ERROR;
                        message = "Cassandra Daemon exited with abnormal " +
                                "status " + exitCode;
                    }

                    CassandraDaemonStatus status = CassandraDaemonStatus.create(
                            state,
                            task.getId(),
                            task.getSlaveId(),
                            task.getExecutor().getId(),
                            Optional.of(message),
                            task.getStatus().getMode());
                    driver.sendStatusUpdate(status.toProto());
                    LOGGER.info("Sent status update: status = {}", status);
                    open.set(false);
                    closeFuture.complete(CLOSED);
                    System.exit(0);


                } catch (InterruptedException ex) {

                } catch (Throwable t) {
                    t.printStackTrace();
                    return;
                }
            }
        }
    }

    private static final class ModeReporter implements Runnable {

        public static ModeReporter create(
                final CassandraDaemonTask task,
                final NodeProbe probe,
                final ExecutorDriver driver,
                final AtomicBoolean open,
                final
                AtomicReference<CassandraMode> mode) {
            return new ModeReporter(task, probe, driver, open, mode);
        }

        private final CassandraDaemonTask task;
        private final NodeProbe probe;
        private final ExecutorDriver driver;
        private final AtomicBoolean open;
        private final AtomicReference<CassandraMode> mode;

        private ModeReporter(final CassandraDaemonTask task,
                             final NodeProbe probe,
                             final ExecutorDriver driver,
                             final AtomicBoolean open,
                             final AtomicReference<CassandraMode> mode) {

            this.task = task;
            this.probe = probe;
            this.driver = driver;
            this.open = open;
            this.mode = mode;
        }

        public void run() {

            if (open.get()) {


                CassandraMode current = CassandraMode.valueOf(
                        probe.getOperationMode());
                ;

                if (!mode.get().equals(current)) {
                    mode.set(current);
                    LOGGER.info("Cassandra Daemon mode = {}", current);
                    CassandraDaemonStatus daemonStatus =
                            CassandraDaemonStatus.create(
                                    Protos.TaskState.TASK_RUNNING,
                                    task.getId(),
                                    task.getSlaveId(),
                                    task.getExecutor().getId(),
                                    Optional.empty(),
                                    current
                            );
                    driver.sendStatusUpdate(daemonStatus.toProto());
                    LOGGER.debug("Sent status update = {} ", daemonStatus);
                }
            }
        }
    }

    private static CassandraStatus getCassandraStatus(final NodeProbe probe) {
        return CassandraStatus.create(
                CassandraMode.valueOf(
                        probe.getOperationMode()
                ), probe.isJoined(),
                probe.isThriftServerRunning(),
                probe.isNativeTransportRunning(),
                probe.isInitialized(),
                probe.isGossipRunning(),
                probe.getLocalHostId(),
                probe.getEndpoint(),
                probe.getTokens().size(),
                probe.getDataCenter(),
                probe.getRack(),
                probe.getReleaseVersion());
    }

    private static final String getListenAddress() throws UnknownHostException {

        String address = System.getenv("LIBPROCESS_IP");

        if (address == null || address.isEmpty()) {
            address = InetAddress.getLocalHost().getHostAddress();
            LOGGER.warn("LIBPROCESS_IP address not found defaulting to " +
                    "localhost");
        }

        LOGGER.info("Retrieved Cassandra Daemon listen address: address = {}",
                address);

        return address;
    }

    /**
     * Creates a new CassandraDaemonProcess with background status reporting
     * and a process watchdog. After calling this method the Cassandra
     * process is running and the NodeProbe instance is connected.
     *
     * @param task     The CassandraDaemonTask that corresponds to the process.
     * @param executor The ScheduledExecutorService to use for background
     *                 Runnables (The watchdog and status reporter).
     * @param driver   The ExecutorDriver for the CassandraExecutor.
     * @return A CassandraDaemonProcess constructed from the
     * @throws IOException If an error occurs attempting to start the
     *                     CassandraProcess or connect to it via NodeProbe.
     */
    public static final CassandraDaemonProcess create(
            final CassandraDaemonTask task,
            final ScheduledExecutorService executor,
            final ExecutorDriver driver) throws IOException {

        return new CassandraDaemonProcess(task, executor, driver);
    }


    private final CassandraDaemonTask task;
    private final ScheduledExecutorService executor;
    private final CassandraPaths paths;
    private final Process process;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicReference<CassandraMode> mode;
    private final NodeProbe probe;
    private final CompletableFuture<Object> closeFuture =
            new CompletableFuture<>();
    private final MetricsConfig metricsConfig;
    private boolean metricsEnabled;

    private String getReplaceIp() throws UnknownHostException {
        if (task.getConfig().getReplaceIp().trim().isEmpty()) {
            return "";
        } else {
            InetAddress address =
                    InetAddress.getByName(task.getConfig().getReplaceIp());
            LOGGER.info("Replacing node: address = {}", address);
            return "-Dcassandra.replace_address=" + address.getHostAddress();
        }
    }

    private Process createDaemon() throws IOException {

        final ProcessBuilder builder = new ProcessBuilder(
                paths.cassandraRun()
                        .toString(),
                getReplaceIp(),
                "-f"
        )
                .directory(new File(System.getProperty("user.dir")))
                .redirectOutput(new File("cassandra-stdout.log"))
                .redirectError(new File("cassandra-stderr.log"));

        builder.environment().putAll(task.getConfig().getHeap().toEnv());
        builder.environment().put(
                "JMX_PORT",
                Integer.toString(task.getConfig().getJmxPort()));
        if (metricsEnabled) {
            metricsConfig.setEnv(builder.environment());
        }
        return builder.start();
    }

    private NodeProbe connectProbe() {

        NodeProbe nodeProbe;

        while (open.get()) {
            try {
                nodeProbe = new NodeProbe("127.0.0.1",
                        task.getConfig().getJmxPort());
                LOGGER.info("Node probe is successfully connected to the " +
                                "Cassandra Daemon: port {}",
                        task.getConfig().getJmxPort());
                return nodeProbe;
            } catch (Exception ex) {

                LOGGER.info("Connection to server failed backing off for 500 " +
                        "ms");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }

            }
        }

        throw new IllegalStateException(
                String.format("Failed to connect to Cassandra " +
                        "Daemon: port = %s", task.getConfig().getJmxPort()));
    }

    /**
     * Consructs a new CassandraDaemonProcess with background status reporting
     * and a process watchdog. After calling this method the Cassandra
     * process is running and the NodeProbe instance is connected.
     *
     * @param task     The CassandraDaemonTask that corresponds to the process.
     * @param executor The ScheduledExecutorService to use for background
     *                 Runnables (The watchdog and status reporter).
     * @param driver   The ExecutorDriver for the CassandraExecutor
     * @throws IOException If an error occurs attempting to start the
     *                     CassandraProcess or connect to it via NodeProbe.
     */
    public CassandraDaemonProcess(final CassandraDaemonTask task,
                                  final ScheduledExecutorService executor,
                                  final ExecutorDriver driver)
            throws IOException {

        this.task = task;
        this.executor = executor;
        this.paths = CassandraPaths.create(
                task.getConfig().getVersion());
        task.getConfig().getLocation().writeProperties(
                paths.cassandraLocation());

        task.getConfig().getApplication().toBuilder()
                .setPersistentVolume(Paths.get("").resolve(task.getConfig()
                        .getVolume()
                        .getPath()).toAbsolutePath().toString())
                .setListenAddress(getListenAddress())
                .setRpcAddress(getListenAddress())
                .build().writeDaemonConfiguration(paths.cassandraConfig());

        this.metricsConfig = new MetricsConfig(task.getExecutor());
        metricsEnabled = metricsConfig.metricsEnabled();
        if (metricsEnabled) {
            metricsEnabled = metricsConfig.writeMetricsConfig(paths.conf());
        }

        process = createDaemon();
        executor.submit(
                WatchDog.create(task, process, driver, open, closeFuture));
        probe = connectProbe();
        CassandraMode current = CassandraMode.valueOf(probe.getOperationMode());
        mode = new AtomicReference<>(current);

        CassandraDaemonStatus daemonStatus =
                CassandraDaemonStatus.create(
                        Protos.TaskState.TASK_RUNNING,
                        task.getId(),
                        task.getSlaveId(),
                        task.getExecutor().getId(),
                        Optional.empty(),
                        current);
        driver.sendStatusUpdate(daemonStatus.toProto());
        LOGGER.debug("Sent status update = {} ", daemonStatus);
        executor.scheduleAtFixedRate(
                ModeReporter.create(task,
                        probe,
                        driver,
                        open,
                        mode),
                1, 1, TimeUnit.SECONDS);
    }


    /**
     * Gets the NodeProbe.
     *
     * @return The NodeProbe instance used to communicate with the Cassandra
     * process.
     */
    public NodeProbe getProbe() {
        return this.probe;
    }

    /**
     * Gets the task.
     *
     * @return The CassandraDaemonTask that is the Mesos abstraction of the
     * Cassandra process.
     */
    public CassandraDaemonTask getTask() {
        return task;
    }

    /**
     * Gets the mode.
     *
     * @return The CassandraMode for the Cassandra daemon.
     */
    public CassandraMode getMode() {
        return mode.get();
    }

    /**
     * Gets the status.
     *
     * @return The status of the Cassandra daemon.
     */
    public CassandraStatus getStatus() {

        return getCassandraStatus(probe);
    }

    /**
     * Gets the status of the Java process.
     *
     * @return True if the Java process for the Cassandra Daemon is running.
     */
    public boolean isOpen() {
        return open.get();
    }

    /**
     * Gets the key spaces.
     *
     * @return A list of the names of all of the key spaces for the Cassandra
     * instance.
     */
    public List<String> getKeySpaces() {
        return probe.getKeyspaces();
    }

    /**
     * Gets the non-system key spaces.
     *
     * @return A list of the names of all of the non-system key spaces for
     * the Cassandra instance.
     */
    public List<String> getNonSystemKeySpaces() {
        return probe.getNonSystemKeyspaces();
    }

    /**
     * Shuts the Cassandra process down and causes the Executor to exit.
     */
    public void shutdown() {

        try {
            probe.stopCassandraDaemon();
        } catch (Throwable expected) {

            expected.printStackTrace();
        }

        while (true) {
            try {
                process.waitFor();
                return;
            } catch (InterruptedException e) {

            }
        }
    }

    /**
     * Forcibly kills (as in kill -9) the Cassandra process causing the
     * executor to shutdown.
     */
    public void kill() {
        try {
            process.destroyForcibly();
        } catch (Throwable expected) {
            expected.printStackTrace();
        }

        while (true) {
            try {
                process.waitFor();
                return;
            } catch (InterruptedException e) {

            }
        }
    }

    /**
     * Assassinates the node at address.
     *
     * @param address The string ip address or hostname of the node to
     *                assassinate.
     * @throws UnknownHostException If the address of the node can not be
     *                              resolved.
     */
    public void assassinate(String address) throws UnknownHostException {
        this.probe.assassinateEndpoint(address);
    }

    /**
     * Cleans the deleted keys and keys that no longer belong to the node for
     * the indicated key space and column families.
     *
     * @param keySpace       The key space to cleanup.
     * @param columnFamilies A list of the column families to clean. If
     *                       empty, all column families are cleaned.
     * @throws InterruptedException If the task is interrupted.
     * @throws ExecutionException   If execution fails.
     * @throws IOException          If an IOException occurs communicating with the
     *                              process.
     */
    public void cleanup(String keySpace,
                        List<String> columnFamilies)
            throws InterruptedException, ExecutionException, IOException {

        if (columnFamilies.isEmpty()) {
            this.probe.forceKeyspaceCleanup(keySpace);
        } else {
            String[] families = new String[columnFamilies.size()];
            families = columnFamilies.toArray(families);
            this.probe.forceKeyspaceCleanup(keySpace, families);
        }
    }

    /**
     * Cleans the deleted keys and keys that no longer belong to the node for
     * for all key spaces and column families.
     *
     * @throws InterruptedException If the task is interrupted.
     * @throws ExecutionException   If execution fails.
     * @throws IOException          If an IOException occurs communicating with the
     *                              process.
     */
    public void cleanup()
            throws InterruptedException, ExecutionException, IOException {


        this.probe.forceKeyspaceCleanup(null);

    }

    /**
     * Takes a snapshot of the indicated key space with the given name.
     *
     * @param name     The name of the snapshot.
     * @param keySpace The name of the key space.
     * @throws IOException If an error occurs taking the snapshot.
     */
    public void takeSnapShot(String name, String keySpace) throws IOException {
        probe.takeSnapshot(name, null, keySpace);
    }

    /**
     * Performs anti-entropy repair on the indicated keySpace.
     *
     * @param keySpace The keyspace that will be repaired.
     * @param options  The options for the repair operation.
     * @return The output of the repair operation.
     * @throws IOException If an error occurs executing the repair or
     *                     parsing the output.
     */
    public String repair(String keySpace, Map<String, String> options)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);

        probe.repairAsync(out, keySpace, options);

        return baos.toString("UTF8");
    }

    /**
     * Clears a snapshot for the given key spaces.
     *
     * @param name      The name of the snap shot.
     * @param keySpaces The key spaces to clear. If empty, all key spaces are
     *                  cleared
     * @throws IOException If the clear fails.
     */
    public void clearSnapShot(String name, String... keySpaces) throws
            IOException {
        probe.clearSnapshot(name, keySpaces);
    }

    /**
     * Decommissions the node. Leaving the node as a ring member that is not
     * responsible for a token range and that is ready to be removed.
     *
     * @throws InterruptedException If decommission fails.
     */
    public void decommission() throws InterruptedException {
        this.probe.decommission();
    }

    /**
     * Drains all client connections from the node.
     *
     * @throws InterruptedException If the drain is interrupted.
     * @throws ExecutionException   If an exception occurs during execution of
     *                              the drain
     * @throws IOException          If communication with the node fails.
     */
    public void drain()
            throws InterruptedException, ExecutionException, IOException {
        this.probe.drain();
    }

    /**
     * Upgrades the SSTables from a previous version to the version
     * corresponding to the current version of Cassandra. Once this is
     * invoked the tables can not be downgraded.
     *
     * @throws InterruptedException If the upgrade is interrupted.
     * @throws ExecutionException   If execution is interrupted.
     * @throws IOException          If communication with Cassandra fails.
     */
    public void upgradeTables()
            throws InterruptedException, ExecutionException, IOException {

        this.probe.upgradeSSTables(null, true);
    }


}
