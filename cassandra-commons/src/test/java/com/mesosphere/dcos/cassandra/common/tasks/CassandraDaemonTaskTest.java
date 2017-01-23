package com.mesosphere.dcos.cassandra.common.tasks;

import com.mesosphere.dcos.cassandra.common.config.*;
import org.apache.mesos.Protos;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.offer.VolumeRequirement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * This class tests the CassandraDaemonTask class.
 */
public class CassandraDaemonTaskTest {
    private static final String TEST_DAEMON_NAME = "test-daemon-task-name";
    private static final UUID TEST_CONFIG_ID = UUID.randomUUID();
    public static final String TEST_CONFIG_NAME = TEST_CONFIG_ID.toString();

    private CassandraDaemonTask.Factory testTaskFactory;
    private ExecutorConfig testExecutorConfig;
    private CassandraTaskExecutor testTaskExecutor;

    @Before
    public void beforeEach() throws URISyntaxException, IOException {
        Capabilities mockCapabilities = Mockito.mock(Capabilities.class);
        when(mockCapabilities.supportsNamedVips()).thenReturn(true);
        testTaskFactory = new CassandraDaemonTask.Factory(mockCapabilities);
        testExecutorConfig = ExecutorConfig.create(
                "test-cmd",
                Arrays.asList("arg0"),
                1.0,
                256,
                500,
                1000,
                "java-home",
                new URI("http://jre-location"),
                new URI("http://executor-location"),
                new URI("http://cassandra-location"),
                new URI("http://libmesos-location"),
                false);

        testTaskExecutor = CassandraTaskExecutor.create(
                "test-framework-id",
                TEST_DAEMON_NAME,
                "test-role",
                "test-principal",
                testExecutorConfig);
    }

    @Test
    public void testConstructCassandraDaemonTask() {
        Assert.assertNotNull(testTaskFactory.create(
          TEST_DAEMON_NAME,
          TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT));
    }

    @Test
    public void testUpdateUnchangedConfig() {
        CassandraDaemonTask daemonTask = testTaskFactory.create(
          TEST_DAEMON_NAME,
          TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(
                CassandraConfig.DEFAULT,
                testExecutorConfig,
                TEST_CONFIG_ID);
        Assert.assertEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
    }

    @Test
    public void testUpdateCpuConfig() {
        CassandraDaemonTask daemonTask = testTaskFactory.create(
          TEST_DAEMON_NAME,
          TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        double newCpu = 1.0;
        CassandraConfig updatedConfig = CassandraConfig.create(
                "2.2.5",
                newCpu,
                4096,
                10240,
                VolumeRequirement.VolumeType.ROOT,
                "",
                HeapConfig.DEFAULT,
                Location.DEFAULT,
                7199,
                false,
                UUID.randomUUID().toString(),
                CassandraApplicationConfig.builder().build());

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(
                updatedConfig,
                testExecutorConfig,
                TEST_CONFIG_ID);
        Assert.assertNotEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
        Assert.assertEquals(newCpu, updatedTask.getConfig().getCpus(), 0.0);
        Assert.assertTrue(allUrisAreCacheable(updatedTask.getTaskInfo().getExecutor().getCommand().getUrisList(), false));
    }

    @Test
    public void testUpdateMemConfig() {
        CassandraDaemonTask daemonTask = testTaskFactory.create(
          TEST_DAEMON_NAME,
          TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        int newMem = 1000;
        CassandraConfig updatedConfig = CassandraConfig.create(
                "2.2.5",
                0.2,
                newMem,
                10240,
                VolumeRequirement.VolumeType.ROOT,
                "",
                HeapConfig.DEFAULT,
                Location.DEFAULT,
                7199,
                false,
                UUID.randomUUID().toString(),
                CassandraApplicationConfig.builder().build());

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(
                updatedConfig,
                testExecutorConfig,
                TEST_CONFIG_ID);
        Assert.assertNotEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
        Assert.assertEquals(newMem, updatedTask.getConfig().getMemoryMb(), 0.0);
        double taskInfoDisk = getScalar(updatedTask.getTaskInfo().getResourcesList(), "mem");
        Assert.assertEquals(newMem, taskInfoDisk, 0.0);
    }

    @Test
    public void testUpdateDiskConfig() {
        CassandraDaemonTask daemonTask = testTaskFactory.create(
          TEST_DAEMON_NAME,
          TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        int newDisk = 5000;
        CassandraConfig updatedConfig = CassandraConfig.create(
                "2.2.5",
                0.2,
                4096,
                newDisk,
                VolumeRequirement.VolumeType.ROOT,
                "",
                HeapConfig.DEFAULT,
                Location.DEFAULT,
                7199,
                false,
                UUID.randomUUID().toString(),
                CassandraApplicationConfig.builder().build());

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(
                updatedConfig,
                testExecutorConfig,
                TEST_CONFIG_ID);
        Assert.assertNotEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
        Assert.assertEquals(newDisk, updatedTask.getConfig().getDiskMb(), 0.0);
        double originalTaskInfoDisk = getScalar(daemonTask.getTaskInfo().getResourcesList(), "disk");
        double updatedTaskInfoDisk = getScalar(updatedTask.getTaskInfo().getResourcesList(), "disk");
        // Updating the Disk should not result in updated disk.  Disk cannot be updated.
        Assert.assertEquals(originalTaskInfoDisk, updatedTaskInfoDisk, 0.0);
    }

    @Test
    public void testUpdateCassandraLocation() throws URISyntaxException {
        CassandraDaemonTask daemonTask = testTaskFactory.create(
                TEST_DAEMON_NAME,
                TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        ExecutorConfig updatedTestExecutorConfig = ExecutorConfig.create(
                "test-cmd",
                Arrays.asList("arg0"),
                1.0,
                256,
                500,
                1000,
                "java-home",
                new URI("http://jre-location"),
                new URI("http://executor-location"),
                new URI("http://cassandra-location-updated"),
                new URI("http://libmesos-location"),
                false);

        testTaskExecutor = CassandraTaskExecutor.create(
                "test-framework-id",
                TEST_DAEMON_NAME,
                "test-role",
                "test-principal",
                updatedTestExecutorConfig);

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(
                CassandraConfig.DEFAULT,
                updatedTestExecutorConfig,
                TEST_CONFIG_ID);
        Assert.assertNotEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
        Assert.assertEquals(4, updatedTask.getExecutor().getURIs().size());
        Assert.assertTrue(updatedTask.getExecutor().getURIs().contains("http://cassandra-location-updated"));
    }

    @Test
    public void testPublishDiscoveryInfo() {
        CassandraConfig cassandraConfig = CassandraConfig.builder().setPublishDiscoveryInfo(true).build();

        CassandraDaemonTask daemonTask = testTaskFactory.create(
                TEST_DAEMON_NAME,
                TEST_CONFIG_NAME,
                testTaskExecutor,
                cassandraConfig);

        Protos.DiscoveryInfo discovery = daemonTask.getTaskInfo().getDiscovery();
        Assert.assertEquals("Test Cluster.test-daemon-task-name", discovery.getName());
        Assert.assertEquals(Protos.DiscoveryInfo.Visibility.EXTERNAL, discovery.getVisibility());
        Assert.assertEquals(1, discovery.getPorts().getPortsCount());
        Assert.assertEquals(9042, discovery.getPorts().getPorts(0).getNumber());
        Assert.assertEquals("NativeTransport", discovery.getPorts().getPorts(0).getName());
    }

    @Test
    public void testDcosNamedVipDiscoveryInfo() {
        CassandraDaemonTask daemonTask = testTaskFactory.create(
                TEST_DAEMON_NAME,
                TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        Protos.DiscoveryInfo discovery = daemonTask.getTaskInfo().getDiscovery();
        Assert.assertEquals("test-daemon-task-name", discovery.getName());
        Assert.assertEquals(Protos.DiscoveryInfo.Visibility.EXTERNAL, discovery.getVisibility());
        Assert.assertEquals(1, discovery.getPorts().getPortsCount());
        Assert.assertEquals(9042, discovery.getPorts().getPorts(0).getNumber());
        Assert.assertEquals("tcp", discovery.getPorts().getPorts(0).getProtocol());
    }

    @Test
    public void testUpdateCacheFetchedUris() throws URISyntaxException {
        CassandraDaemonTask daemonTask = testTaskFactory.create(
                TEST_DAEMON_NAME,
                TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        ExecutorConfig updatedTestExecutorConfig = ExecutorConfig.create(
                "test-cmd",
                Arrays.asList("arg0"),
                1.0,
                256,
                500,
                1000,
                "java-home",
                new URI("http://jre-location"),
                new URI("http://executor-location"),
                new URI("http://cassandra-location"),
                new URI("http://libmesos-location"),
                /* cacheFetchedUris */ true);

        testTaskExecutor = CassandraTaskExecutor.create(
                "test-framework-id",
                TEST_DAEMON_NAME,
                "test-role",
                "test-principal",
                updatedTestExecutorConfig);

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(
                CassandraConfig.DEFAULT,
                updatedTestExecutorConfig,
                TEST_CONFIG_ID);
        Assert.assertTrue(allUrisAreCacheable(updatedTask.getTaskInfo().getExecutor().getCommand().getUrisList(), true));
    }


    @Test
    public void testUpdateLibmesosLocation() throws URISyntaxException, UnsupportedEncodingException {
        // Before the introduction of libmesosLocation variable, the executor config stored in zookeeper has
        // null libmesosLocation and should be constructed and deserialized correctly.
        ExecutorConfig testExecutorConfig = ExecutorConfig.create(
                "test-cmd",
                Arrays.asList("arg0"),
                1.0,
                256,
                500,
                1000,
                "java-home",
                "http://jre-location",
                "http://executor-location",
                "http://cassandra-location",
                /* libmesosLocation */ null,
                false,
                false);

        testTaskExecutor = CassandraTaskExecutor.create(
                "test-framework-id",
                TEST_DAEMON_NAME,
                "test-role",
                "test-principal",
                testExecutorConfig);

        CassandraDaemonTask daemonTask = testTaskFactory.create(
                TEST_DAEMON_NAME,
                TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);
        Assert.assertTrue(daemonTask.getExecutor().getURIs().contains(ExecutorConfig.DEFAULT_LIBMESOS_LOCATION));

        ExecutorConfig updatedTestExecutorConfig = ExecutorConfig.create(
                "test-cmd",
                Arrays.asList("arg0"),
                1.0,
                256,
                500,
                1000,
                "java-home",
                "http://jre-location",
                "http://executor-location",
                "http://cassandra-location",
                "http://libmesos-location-new",
                false,
                false);

        testTaskExecutor = CassandraTaskExecutor.create(
                "test-framework-id",
                TEST_DAEMON_NAME,
                "test-role",
                "test-principal",
                updatedTestExecutorConfig);

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(
                CassandraConfig.DEFAULT,
                updatedTestExecutorConfig,
                TEST_CONFIG_ID);
        Assert.assertTrue(updatedTask.getExecutor().getURIs().contains("http://libmesos-location-new"));
    }

    private Protos.TaskInfo normalizeCassandraTaskInfo(CassandraDaemonTask daemonTask) {
        Protos.TaskInfo daemonTaskInfo = daemonTask.getTaskInfo();
        Protos.ExecutorInfo expectedExecutorInfo = Protos.ExecutorInfo.newBuilder(daemonTaskInfo.getExecutor())
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(""))
                .build();
        daemonTaskInfo = Protos.TaskInfo.newBuilder(daemonTaskInfo)
                .setTaskId(Protos.TaskID.newBuilder().setValue(""))
                .setExecutor(expectedExecutorInfo)
                .build();
        return daemonTaskInfo;
    }

    private Double getScalar(List<Protos.Resource> resources, String name) {
        for (Protos.Resource resource : resources) {
            if (resource.getName().equals(name)) {
                return resource.getScalar().getValue();
            }
        }

        return null;
    }

    // Helper method that returns true iff all the @param uris in the given list have the cache property set to the
    // given @param cacheable value.
    private boolean allUrisAreCacheable(List<Protos.CommandInfo.URI> uris, boolean cacheable) {
        for (Protos.CommandInfo.URI uri : uris) {
            if (uri.getCache() != cacheable) {
                return false;
            }
        }
        return true;
    }
}
