package com.mesosphere.dcos.cassandra.common.tasks;

import com.mesosphere.dcos.cassandra.common.config.*;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.VolumeRequirement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * This class tests the CassandraDaemonTask class.
 */
public class CassandraDaemonTaskTest {
    private static final String TEST_DAEMON_NAME = "test-daemon-task-name";
    private static final UUID TEST_CONFIG_ID = UUID.randomUUID();
    public static final String TEST_CONFIG_NAME = TEST_CONFIG_ID.toString();

    private ExecutorConfig testExecutorConfig;
    private CassandraTaskExecutor testTaskExecutor;

    @Before
    public void beforeEach() throws URISyntaxException {
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
                "ulimit-mem-lock",
                "ulimit-no-file",
                "ulimit-nproc");

        testTaskExecutor = CassandraTaskExecutor.create(
                "test-framework-id",
          TEST_DAEMON_NAME,
                "test-role",
                "test-principal",
                testExecutorConfig);
    }

    @Test
    public void testConstructCassandraDaemonTask() {
        Assert.assertNotNull(CassandraDaemonTask.create(
          TEST_DAEMON_NAME,
          TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT));
    }

    @Test
    public void testUpdateUnchangedConfig() {
        CassandraDaemonTask daemonTask = CassandraDaemonTask.create(
          TEST_DAEMON_NAME,
          TEST_CONFIG_NAME,
                testTaskExecutor,
                CassandraConfig.DEFAULT);

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(CassandraConfig.DEFAULT,TEST_CONFIG_ID);
        Assert.assertEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
    }

    @Test
    public void testUpdateCpuConfig() {
        CassandraDaemonTask daemonTask = CassandraDaemonTask.create(
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
                CassandraApplicationConfig.builder().build());

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(updatedConfig,TEST_CONFIG_ID);
        Assert.assertNotEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
        Assert.assertEquals(newCpu, updatedTask.getConfig().getCpus(), 0.0);
    }

    @Test
    public void testUpdateMemConfig() {
        CassandraDaemonTask daemonTask = CassandraDaemonTask.create(
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
                CassandraApplicationConfig.builder().build());

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(updatedConfig,TEST_CONFIG_ID);
        Assert.assertNotEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
        Assert.assertEquals(newMem, updatedTask.getConfig().getMemoryMb(), 0.0);
        double taskInfoDisk = getScalar(updatedTask.getTaskInfo().getResourcesList(), "mem");
        Assert.assertEquals(newMem, taskInfoDisk, 0.0);
    }

    @Test
    public void testUpdateDiskConfig() {
        CassandraDaemonTask daemonTask = CassandraDaemonTask.create(
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
                CassandraApplicationConfig.builder().build());

        CassandraDaemonTask updatedTask = daemonTask.updateConfig(updatedConfig,TEST_CONFIG_ID);
        Assert.assertNotEquals(normalizeCassandraTaskInfo(daemonTask), normalizeCassandraTaskInfo(updatedTask));
        Assert.assertEquals(newDisk, updatedTask.getConfig().getDiskMb(), 0.0);
        double originalTaskInfoDisk = getScalar(daemonTask.getTaskInfo().getResourcesList(), "disk");
        double updatedTaskInfoDisk = getScalar(updatedTask.getTaskInfo().getResourcesList(), "disk");
        // Updating the Disk should not result in updated disk.  Disk cannot be updated.
        Assert.assertEquals(originalTaskInfoDisk, updatedTaskInfoDisk, 0.0);
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
}
