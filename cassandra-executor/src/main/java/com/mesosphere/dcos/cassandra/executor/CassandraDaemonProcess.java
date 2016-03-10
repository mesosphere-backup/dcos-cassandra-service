package com.mesosphere.dcos.cassandra.executor;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonStatus;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
                    LOGGER.info("Cassandra Daemon terminated exit value = {}",
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
                    LOGGER.info("Sent status update = {}", status);
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

    public static final CassandraDaemonProcess create(
            final CassandraDaemonTask task,
            final ScheduledExecutorService executor,
            final ExecutorDriver driver) throws IOException {

        return new CassandraDaemonProcess(task, executor, driver);
    }

    private static final String getListenAddress() throws UnknownHostException {

        String address = System.getenv("LIBPROCESS_IP");

        if (address == null || address.isEmpty()) {
            address = InetAddress.getLocalHost().getHostAddress();
            LOGGER.info("LIBPROCESS_IP address not found defaulting to " +
                    "localhost");
        }

        LOGGER.info("Cassandra Daemon listen address = {}", address);

        return address;
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
        LOGGER.info("Sent status update = {} ", daemonStatus);
        executor.scheduleAtFixedRate(
                ModeReporter.create(task,
                        probe,
                        driver,
                        open,
                        mode),
                1, 1, TimeUnit.SECONDS);
    }

    public NodeProbe getProbe() {
        return this.probe;
    }

    private String getReplaceIp() throws UnknownHostException {
        if(task.getConfig().getReplaceIp().trim().isEmpty()){
            return "";
        } else {
            InetAddress address =
                    InetAddress.getByName(
                    task.getConfig().getReplaceIp());
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
        return builder.start();
    }

    private NodeProbe connectProbe() {

        NodeProbe nodeProbe;

        while (open.get()) {
            try {
                nodeProbe = new NodeProbe("127.0.0.1",
                        task.getConfig().getJmxPort());
                LOGGER.info("Node probe is successfully connected to the " +
                                "Cassandra Daemon on port {}",
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
                        "Daemon on port %s", task.getConfig().getJmxPort()));
    }

    public CassandraDaemonTask getTask() {
        return task;
    }

    public CassandraMode getMode() {
        return mode.get();
    }

    public CassandraStatus getStatus() {

        return getCassandraStatus(probe);
    }

    public boolean isOpen() {
        return open.get();
    }

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

    public void assassinate(String address) throws UnknownHostException {
        this.probe.assassinateEndpoint(address);
    }

    public void cleanup(Optional<String> keySpaceOption,
                        List<String> columnFamilies)
            throws InterruptedException, ExecutionException, IOException {
        String keySpace = (keySpaceOption.isPresent()) ?
                keySpaceOption.get() : null;

        if (columnFamilies.isEmpty()) {
            this.probe.forceKeyspaceCleanup(keySpace);
        } else {
            String[] families = new String[columnFamilies.size()];
            families = columnFamilies.toArray(families);
            this.probe.forceKeyspaceCleanup(keySpace, families);
        }
    }

    public void decommission() throws InterruptedException {
        this.probe.decommission();
    }

    public void drain()
            throws InterruptedException, ExecutionException, IOException {
        this.probe.drain();
    }

    public void upgradeTables()
            throws InterruptedException, ExecutionException, IOException {

        this.probe.upgradeSSTables(null,true);
    }


}
