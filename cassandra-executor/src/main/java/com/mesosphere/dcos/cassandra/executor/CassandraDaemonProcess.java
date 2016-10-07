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


import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mesosphere.dcos.cassandra.common.tasks.*;
import com.mesosphere.dcos.cassandra.executor.metrics.MetricsConfig;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ProcessTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * The CassandraDaemonProcess launches the Cassandra process process,
 * monitors its current mode and status, and reports changes to the scheduler
 * . If the Cassandra daemon terminates the CassandraDaemonProcess causes the
 * executor to exit.
 * All administration and monitoring is achieved by attaching to the Cassandra
 * daemon via JMX using the NodeProbe class.
 */
public class CassandraDaemonProcess extends ProcessTask {
    public static final Set<String> SYSTEM_KEYSPACE_NAMES =
            ImmutableSet.of(SystemKeyspace.NAME, SchemaKeyspace.NAME);
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDaemonProcess.class);

    private static final Object CLOSED = new Object();
    private final CassandraDaemonTask task;
    private final CassandraPaths paths;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicReference<CassandraMode> mode;
    private final Probe probe;

    private static final class ModeReporter implements Runnable {

        private final CassandraDaemonTask task;
        private final ExecutorDriver driver;
        private final AtomicBoolean open;
        private final AtomicReference<CassandraMode> mode;
        private final Probe probe;

        public static ModeReporter create(
                final CassandraDaemonTask task,
                final Probe probe,
                final ExecutorDriver driver,
                final AtomicBoolean open,
                final
                AtomicReference<CassandraMode> mode) {
            return new ModeReporter(task, probe, driver, open, mode);
        }

        private ModeReporter(
                final CassandraDaemonTask task,
                final Probe probe,
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
                CassandraMode current = CassandraMode.valueOf(probe.get().getOperationMode());
                if (!mode.get().equals(current)) {
                    mode.set(current);
                    LOGGER.info("Cassandra Daemon mode = {}", current);
                    CassandraDaemonStatus daemonStatus =
                            task.createStatus(Protos.TaskState.TASK_RUNNING,
                                    mode.get(),
                                    Optional.of("Cassandra Daemon running."));
                    driver.sendStatusUpdate(daemonStatus.getTaskStatus());
                    LOGGER.info("Sent status update = {} ", daemonStatus);
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
            final ScheduledExecutorService scheduledExecutorService,
            final Protos.TaskInfo taskInfo,
            final ExecutorDriver driver) throws IOException {

        CassandraDaemonTask cassandraTask = (CassandraDaemonTask) CassandraTask.parse(taskInfo);
        CassandraPaths cassandraPaths = CassandraPaths.create(cassandraTask.getConfig().getVersion());
        cassandraTask.getConfig().getLocation().writeProperties(cassandraPaths.cassandraLocation());

        cassandraTask.getConfig().getApplication().toBuilder()
                .setListenAddress(getListenAddress())
                .setRpcAddress(getListenAddress())
                .build().writeDaemonConfiguration(cassandraPaths.cassandraConfig());

        cassandraTask.getConfig().getHeap().writeHeapSettings(cassandraPaths.heapConfig());


        ProcessBuilder processBuilder = createDaemon(cassandraPaths, cassandraTask, MetricsConfig.writeMetricsConfig(cassandraPaths.conf()));

        return new CassandraDaemonProcess(scheduledExecutorService, cassandraTask, cassandraPaths, driver, taskInfo, processBuilder, true);
    }

    protected CassandraDaemonProcess(
            ScheduledExecutorService scheduledExecutorService,
            CassandraDaemonTask cassandraTask,
            CassandraPaths cassandraPaths,
            ExecutorDriver executorDriver,
            Protos.TaskInfo taskInfo,
            ProcessBuilder processBuilder,
            boolean exitOnTermination) throws InvalidProtocolBufferException {
        super(executorDriver, taskInfo, processBuilder, exitOnTermination);
        this.task = cassandraTask;
        this.paths = cassandraPaths;

        this.probe = new Probe(cassandraTask);
        this.mode = new AtomicReference<>(CassandraMode.STARTING);
        scheduledExecutorService.scheduleAtFixedRate(
                ModeReporter.create(task,
                        probe,
                        executorDriver,
                        open,
                        mode),
                1, 1, TimeUnit.SECONDS);
    }

    private static String getReplaceIp(CassandraDaemonTask cassandraDaemonTask) throws UnknownHostException {
        if (cassandraDaemonTask.getConfig().getReplaceIp().trim().isEmpty()) {
            return "";
        } else {
            InetAddress address =
                    InetAddress.getByName(cassandraDaemonTask.getConfig().getReplaceIp());
            LOGGER.info("Replacing node: address = {}", address);
            return "-Dcassandra.replace_address=" + address.getHostAddress();
        }
    }

    private static ProcessBuilder createDaemon(CassandraPaths cassandraPaths, CassandraDaemonTask cassandraDaemonTask, boolean metricsEnabled) throws IOException {

        final ProcessBuilder builder = new ProcessBuilder(
            cassandraPaths.cassandraRun().toString(),
            getReplaceIp(cassandraDaemonTask),
            "-f")
            .inheritIO()
            .directory(new File(System.getProperty("user.dir")));
        builder.environment().put(
                "JMX_PORT",
                Integer.toString(cassandraDaemonTask.getConfig().getJmxPort()));
        if (metricsEnabled) {
            MetricsConfig.setEnv(builder.environment());
        }
        return builder;
    }

    /**
     * Gets the NodeProbe.
     *
     * @return The NodeProbe instance used to communicate with the Cassandra
     * process.
     */
    public NodeProbe getProbe() {
        return this.probe.get();
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

        return getCassandraStatus(getProbe());
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
        return getProbe().getKeyspaces();
    }

    /**
     * Gets the non-system key spaces.
     *
     * @return A list of the names of all of the non-system key spaces for
     * the Cassandra instance.
     */
    public List<String> getNonSystemKeySpaces() {
        return getProbe().getKeyspaces().stream().filter(
                keyspace ->
                        !SYSTEM_KEYSPACE_NAMES.contains(keyspace))
                .collect(Collectors.toList());
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
        getProbe().assassinateEndpoint(address);
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
            getProbe().forceKeyspaceCleanup(0, keySpace);
        } else {
            String[] families = new String[columnFamilies.size()];
            families = columnFamilies.toArray(families);
            getProbe().forceKeyspaceCleanup(0, keySpace, families);
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
        for (String keyspace : getNonSystemKeySpaces()) {
            cleanup(keyspace, Collections.emptyList());
        }
    }

    /**
     * Takes a snapshot of the indicated key space with the given name.
     *
     * @param name     The name of the snapshot.
     * @param keySpace The name of the key space.
     * @throws IOException If an error occurs taking the snapshot.
     */
    public void takeSnapShot(String name, String keySpace) throws IOException {
        getProbe().takeSnapshot(name, null, keySpace);
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

        getProbe().repairAsync(out, keySpace, options);

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
        getProbe().clearSnapshot(name, keySpaces);
    }

    /**
     * Decommissions the node. Leaving the node as a ring member that is not
     * responsible for a token range and that is ready to be removed.
     *
     * @throws InterruptedException If decommission fails.
     */
    public void decommission() throws InterruptedException {
        getProbe().decommission();
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
        getProbe().drain();
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
        for (String keyspace : getNonSystemKeySpaces()) {
            getProbe().forceKeyspaceCleanup(0, keyspace);
        }
    }
}
