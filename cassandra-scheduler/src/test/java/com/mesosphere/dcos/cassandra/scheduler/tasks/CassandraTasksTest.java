package com.mesosphere.dcos.cassandra.scheduler.tasks;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.TestData;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.protobuf.OfferBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by kowens on 2/8/16.
 */
public class CassandraTasksTest {

    private static TestingServer server;

    private static CuratorFramework curator;

    private static ZooKeeperPersistence persistence;

    private static CassandraSchedulerConfiguration config;

    private static IdentityManager identity;

    private static ConfigurationManager configuration;

    private static String path;

    private static String path(String id){

        return path + "/" + id;
    }


    @BeforeClass
    public static void beforeAll() throws Exception {

        server = new TestingServer();

        server.start();

        final ConfigurationFactory<CassandraSchedulerConfiguration> factory =
                new ConfigurationFactory<>(
                        CassandraSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(
                                new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");

        config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false)),
                Resources.getResource("scheduler.yml").getFile());

        Identity initial = config.getIdentity();

        persistence = (ZooKeeperPersistence) ZooKeeperPersistence.create(
                initial,
                CuratorFrameworkConfig.create(server.getConnectString(),
                        10000L,
                        10000L,
                        Optional.empty(),
                        250L));

        curator = persistence.getCurator();

        identity = new IdentityManager(
                initial,
                persistence,
                Identity.JSON_SERIALIZER);

        identity.register("test_id");

        configuration = new ConfigurationManager(
                config.getCassandraConfig(),
                config.getExecutorConfig(),
                config.getServers(),
                config.getSeeds(),
                false,
                "NODE",
                "INSTALL",
                config.getSeedsUrl(),
                persistence,
                CassandraConfig.JSON_SERIALIZER,
                ExecutorConfig.JSON_SERIALIZER,
                IntegerStringSerializer.get()
        );

        path = "/" + config.getIdentity().getName() + "/" +
                config.getIdentity().getCluster() + "/tasks";
    }

    @Test
    public void createTask() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask task = tasks.createDaemon();
        assertEquals(1, tasks.get().size());

        assertEquals(task, tasks.get(task.getId()).get());

        assertEquals(task,
                CassandraTask.JSON_SERIALIZER.deserialize(
                        curator.getData().forPath(path(task.getId()))));
    }

    @Test
    public void retrieveTaskOnRestart() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask task = tasks.createDaemon();
        assertEquals(1, tasks.get().size());

        assertEquals(task, tasks.get(task.getId()).get());

        assertEquals(task,
                CassandraTask.JSON_SERIALIZER.deserialize(
                        curator.getData().forPath(path(task.getId()))));

        tasks.stop();

        tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(1, tasks.get().size());
        assertEquals("server-1", tasks.createDaemon().getName());
        assertEquals(2, tasks.get().size());

        assertEquals(task, tasks.get(task.getId()).get());
    }

    @Test
    public void protoSerializable() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask task = tasks.createDaemon();
        assertEquals(1, tasks.get().size());

        assertEquals(task, CassandraDaemonTask.parse(task.toProto()));
    }

    @Test
    public void updateTaskAndGetTaskByNodeId() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask task = tasks.createDaemon();
        assertEquals(1, tasks.get().size());

        CassandraDaemonTask newTask = task.mutable().setCpus(2.3)
                .setDiskMb(456).setHostname("host7").build();
        tasks.update(newTask);
        assertEquals(1, tasks.get().size());

        CassandraTask getTask = tasks.get(newTask.getId()).get();
        assertEquals(2.3, getTask.getCpus(), 0.0001);
        assertEquals(456, getTask.getDiskMb());
        assertEquals("host7", getTask.getHostname());

        CassandraTask get_task2 = tasks.getCassandraTaskByNodeId(0);
        assertEquals(getTask, get_task2);

    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void removeTask() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask task = tasks.createDaemon();
        assertEquals(1, tasks.get().size());

        tasks.remove(task.getId());
        assertEquals(0, tasks.get().size());

        curator.getData().forPath(path(task.getId()));

    }

    @Test
    public void updateTaskWithOffer() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        Protos.Offer offer = TestData.createOffer(
                TestData.randomId(),
                "slave",
                config.getIdentity().getName(),
                "localhost",
                Arrays.asList(
                        ResourceBuilder.reservedCpus(
                                config.getCassandraConfig().getCpus(),
                                config.getIdentity().getRole(),
                                config.getIdentity().getPrincipal()),
                        ResourceBuilder.reservedMem(
                                config.getCassandraConfig().getDiskMb(),
                                config.getIdentity().getRole(),
                                config.getIdentity().getPrincipal()),
                        ResourceBuilder.reservedDisk(
                                config.getCassandraConfig().getDiskMb(),
                                config.getIdentity().getRole(),
                                config.getIdentity().getPrincipal())
                )
        );

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask task = tasks.createDaemon();
        assertEquals(1, tasks.get().size());


        assertEquals(task, CassandraDaemonTask.parse(task.toProto()));

        CassandraTask updated = tasks.update(task.getId(), offer).get();
        assertEquals(1, tasks.get().size());

        assertEquals("slave", updated.getSlaveId());

        assertEquals("localhost", updated.getHostname());

        assertEquals(updated, CassandraTask.JSON_SERIALIZER.deserialize(
                curator.getData().forPath(path(task.getId()))));

    }

    @Test
    public void updateTaskWithStatus() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask task = tasks.createDaemon();
        assertEquals(1, tasks.get().size());

        Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(task.getId()).build())
                .setState(Protos.TaskState.TASK_FINISHED)
                .build();
        tasks.update(taskStatus);
        assertEquals(1, tasks.get().size());

        CassandraTask updatedTask = tasks.get(task.getId()).get();
        assertEquals(Protos.TaskState.TASK_FINISHED, updatedTask.getStatus().getState());

    }

    @Test
    public void getRunningTasksAndTerminatedTasks() throws Exception {

        CassandraTasks tasks = new CassandraTasks(
                identity,
                configuration,
                CassandraTask.JSON_SERIALIZER,
                persistence);

        tasks.start();

        assertEquals(0, tasks.get().size());
        CassandraDaemonTask runningTask1 = tasks.createDaemon();
        assertEquals(1, tasks.get().size());
        Protos.TaskStatus taskStatus1 = Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(runningTask1.getId()).build())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
        tasks.update(taskStatus1);

        CassandraDaemonTask runningTask2 = tasks.createDaemon();
        assertEquals(2, tasks.get().size());
        Protos.TaskStatus taskStatus2 = Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(runningTask2.getId()).build())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
        tasks.update(taskStatus2);


        CassandraDaemonTask terminatedTask1 = tasks.createDaemon();
        assertEquals(3, tasks.get().size());
        Protos.TaskStatus taskStatus3 = Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(terminatedTask1.getId()).build())
                .setState(Protos.TaskState.TASK_FAILED)
                .build();
        tasks.update(taskStatus3);

        CassandraDaemonTask terminatedTask2 = tasks.createDaemon();
        assertEquals(4, tasks.get().size());
        Protos.TaskStatus taskStatus4 = Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(terminatedTask2.getId()).build())
                .setState(Protos.TaskState.TASK_LOST)
                .build();
        tasks.update(taskStatus4);

        CassandraDaemonTask terminatedTask3 = tasks.createDaemon();
        assertEquals(5, tasks.get().size());
        Protos.TaskStatus taskStatus5 = Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(terminatedTask3.getId()).build())
                .setState(Protos.TaskState.TASK_FINISHED)
                .build();
        tasks.update(taskStatus5);

        List<CassandraTask> runningTasks = tasks.getRunningTasks();
        assertEquals(2, runningTasks.size());

        List<CassandraTask> terminatedTasks = tasks.getTerminatedTasks();
        assertEquals(3, terminatedTasks.size());
    }

    @After
    public void afterEach() {

        try {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {

        }
    }

    @AfterClass
    public static void afterAll() throws Exception {

        persistence.stop();

        server.close();

        server.stop();
    }
}
