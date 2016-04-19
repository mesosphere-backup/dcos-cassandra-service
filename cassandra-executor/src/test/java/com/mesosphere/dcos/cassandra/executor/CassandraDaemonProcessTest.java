package com.mesosphere.dcos.cassandra.executor;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonStatus;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskExecutor;
import io.dropwizard.testing.junit.DropwizardClientRule;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class CassandraDaemonProcessTest {

    private static void unpack(File file, Path dir) throws IOException {

        FileInputStream fin = new FileInputStream(file);
        BufferedInputStream in = new BufferedInputStream(fin);
        GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
        TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);

        int BUFFER = 4096;

        TarArchiveEntry entry = null;

        /** Read the tar entries using the getNextEntry method **/

        while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {

            System.out.println("Extracting: " + entry.getName());

            /** If the entry is a directory, create the directory. **/

            if (entry.isDirectory()) {

                File f = dir.resolve(entry.getName()).toFile();
                f.mkdirs();
            }
            /**
             * If the entry is a file,write the decompressed file to the disk
             * and close destination stream.
             **/
            else {
                int count;
                byte data[] = new byte[BUFFER];

                FileOutputStream fos = new FileOutputStream(
                        dir.resolve(entry.getName()).toFile());
                BufferedOutputStream dest = new BufferedOutputStream(fos,
                        BUFFER);
                while ((count = tarIn.read(data, 0, BUFFER)) != -1) {
                    dest.write(data, 0, count);
                }
                dest.close();
            }
        }
    }

    private static String version = "2.2.5";

    private static final Path cassandraTar = Paths.get(Resources.getResource(
            "apache-cassandra-" + version + "-bin.tar.gz")
            .getFile()
    );

    private static final Path cassandra = Paths.get("",
            "apache-cassandra-" + version);

    private static final Path volume = Paths.get("",
            "volume");

    private static final ExecutorDriver driver = mock(ExecutorDriver.class);

    @javax.ws.rs.Path("/seeds")
    @Produces(MediaType.APPLICATION_JSON)
    public static class SeedsResource {
        @GET
        public Map<String, Object> ping() {
            return ImmutableMap.of("isSeed", true, "seeds", Collections
                    .EMPTY_LIST);
        }
    }

    @ClassRule
    public static final DropwizardClientRule dropwizard = new
            DropwizardClientRule(new SeedsResource());


    @BeforeClass
    public static void beforeAll() throws IOException {

        unpack(cassandraTar.toFile(), Paths.get(""));

        cassandra.resolve("bin").resolve("cassandra").toFile().setExecutable
                (true, false);

    }


    @AfterClass
    public static void afterAll() throws IOException {

        FileUtils.deleteDirectory(cassandra.toFile());

        FileUtils.deleteDirectory(volume.toFile());
    }


//    @Test
//    public void testLaunchDaemon() throws IOException, InterruptedException {
//
//        CassandraDaemonTask task = CassandraDaemonTask.create(
//                "server0",
//                "test-slave",
//                InetAddress.getLocalHost().getHostName(),
//                CassandraTaskExecutor.create(
//                        "cassandra",
//                        "executor-test-slave",
//                        "./executor/bin/cassandra-executor",
//                        Collections.emptyList(),
//                        0.1,
//                        1024,
//                        1024,
//                        512,
//                        9000,
//                        9001,
//                        Collections.emptyList(),
//                        "./jre"),
//                "server0",
//                "cassandra",
//                "cassandra",
//                CassandraConfig.DEFAULT.getCpus(),
//                CassandraConfig.DEFAULT.getMemoryMb(),
//                CassandraConfig.DEFAULT.getDiskMb(),
//                CassandraConfig.DEFAULT.mutable().setApplication(
//                        CassandraConfig.DEFAULT.getApplication().toBuilder()
//                        .setSeedProvider(CassandraApplicationConfig.createSimpleSeedProvider(
//                                ImmutableList.of(InetAddress.getLocalHost()
//                                        .getHostAddress())
//                        ))
//                        .build()
//                ).build(),
//                CassandraDaemonStatus.create(Protos.TaskState.TASK_STAGING,
//                        "server0",
//                        "test-slave",
//                        "executor-test-slave",
//                        Optional.empty(),
//                        CassandraMode.STARTING));
//
//
//        CassandraDaemonProcess process =
//                CassandraDaemonProcess.create(task,
//                        Executors.newScheduledThreadPool(10),
//                        driver);
//
//
//        System.out.println(process.getMode());
//
//        assertNotEquals(process.getMode(),
//                CassandraConfig.DEFAULT.getVersion());
//
//        process.kill();
//
//    }

    @Test
    public void testLaunchDaemonDcosSeedProvider() throws IOException,
            InterruptedException {

        CassandraDaemonTask task = CassandraDaemonTask.create(
                "server0",
                "test-slave",
                InetAddress.getLocalHost().getHostName(),
                CassandraTaskExecutor.create(
                        "cassandra",
                        "executor-test-slave",
                        "./executor/bin/cassandra-executor",
                        Collections.emptyList(),
                        0.1,
                        1024,
                        1024,
                        512,
                        9000,
                        Collections.emptyList(),
                        "./jre",
                        true,
                        "statsd",
                        "metrics.prefix.",
                        true,
                        17,
                        "SECONDS",
                        "127.1.2.3",
                        1234),
                "server0",
                "cassandra",
                "cassandra",
                CassandraConfig.DEFAULT.getCpus(),
                CassandraConfig.DEFAULT.getMemoryMb(),
                CassandraConfig.DEFAULT.getDiskMb(),
                CassandraConfig.DEFAULT.mutable().setApplication(
                        CassandraConfig.DEFAULT.getApplication().toBuilder()
                                .setSeedProvider(CassandraApplicationConfig.createDcosSeedProvider(
                                        dropwizard.baseUri().toASCIIString()
                                                + "/seeds"
                                ))
                                .build()
                ).build(),
                CassandraDaemonStatus.create(Protos.TaskState.TASK_STAGING,
                        "server0",
                        "test-slave",
                        "executor-test-slave",
                        Optional.empty(),
                        CassandraMode.STARTING));


        CassandraDaemonProcess process =
                CassandraDaemonProcess.create(task,
                        Executors.newScheduledThreadPool(10),
                        driver);


        System.out.println(process.getMode());

        assertNotEquals(process.getMode(),
                CassandraConfig.DEFAULT.getVersion());

        process.kill();

    }
}
