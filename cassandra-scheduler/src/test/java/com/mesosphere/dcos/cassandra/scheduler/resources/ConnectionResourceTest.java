package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.dcos.Capabilities;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class ConnectionResourceTest {
    private static final String TEST_SERVICE_NAME = "testService";
    private static final int TRANSPORT_PORT = 1234;
    private static final CassandraDaemonTask TEST_TASK_1 = getMockTask("fooname", "foo.com", 1235);
    private static final CassandraDaemonTask TEST_TASK_2 = getMockTask("barname", "bar.com", 1234);
    private static final Map<String, CassandraDaemonTask> TEST_TASKS;
    static {
        TEST_TASKS = new TreeMap<>(); // keep consistent order
        TEST_TASKS.put("foo", TEST_TASK_1);
        TEST_TASKS.put("bar", TEST_TASK_2);
    }

    @Rule
    public final ResourceTestRule resourceWithVips = ResourceTestRule.builder()
            .addResource(getConnectionResource(true)).build();
    @Rule
    public final ResourceTestRule resourceWithoutVips = ResourceTestRule.builder()
            .addResource(getConnectionResource(false)).build();

    @SuppressWarnings("unchecked")
    @Test
    public void testGetConnectWithVips() throws Exception {
        final Map<String, Object> map = (Map<String, Object>) resourceWithVips.client()
                .target("/v1/connection").request().get(Map.class);
        assertEquals(5, map.size());

        final List<String> address = (List<String>) map.get("address");
        assertEquals(map.toString(), 2, address.size());
        assertEquals("bar.com:1234", address.get(0));
        assertEquals("foo.com:1235", address.get(1));

        final List<String> dns = (List<String>) map.get("dns");
        assertEquals(2, dns.size());
        assertEquals("barname.testService.mesos:1234", dns.get(0));
        assertEquals("fooname.testService.mesos:1235", dns.get(1));

        final List<String> hosts = (List<String>) map.get("hosts");
        assertEquals(2, hosts.size());
        assertEquals("bar.com", hosts.get(0));
        assertEquals("foo.com", hosts.get(1));

        assertEquals(TRANSPORT_PORT, map.get("native-port"));

        String vip = map.get("vip").toString();
        assertEquals("node.testService.l4lb.thisdcos.directory:9042", vip);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetConnectWithoutVips() throws Exception {
        final Map<String, Object> map = (Map<String, Object>) resourceWithoutVips.client()
                .target("/v1/connection").request().get(Map.class);
        assertEquals(4, map.size());

        final List<String> address = (List<String>) map.get("address");
        assertEquals(2, address.size());
        assertEquals("bar.com:1234", address.get(0));
        assertEquals("foo.com:1235", address.get(1));

        final List<String> dns = (List<String>) map.get("dns");
        assertEquals(2, dns.size());
        assertEquals("barname.testService.mesos:1234", dns.get(0));
        assertEquals("fooname.testService.mesos:1235", dns.get(1));

        final List<String> ips = (List<String>) map.get("hosts");
        assertEquals(2, ips.size());
        assertEquals("bar.com", ips.get(0));
        assertEquals("foo.com", ips.get(1));

        assertEquals(TRANSPORT_PORT, map.get("native-port"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetAddress() throws Exception {
        final List<String> address = (List<String>) resourceWithVips.client()
                .target("/v1/connection/address").request().get(List.class);
        assertEquals(2, address.size());
        assertEquals("bar.com:1234", address.get(0));
        assertEquals("foo.com:1235", address.get(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetDns() throws Exception {
        final List<String> dns = (List<String>) resourceWithVips.client()
                .target("/v1/connection/dns").request().get(List.class);
        assertEquals(2, dns.size());
        assertEquals("barname.testService.mesos:1234", dns.get(0));
        assertEquals("fooname.testService.mesos:1235", dns.get(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetHosts() throws Exception {
        final List<String> hosts = (List<String>) resourceWithVips.client()
                .target("/v1/connection/hosts").request().get(List.class);
        assertEquals(2, hosts.size());
        assertEquals("bar.com", hosts.get(0));
        assertEquals("foo.com", hosts.get(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPort() throws Exception {
        final int port = resourceWithVips.client()
                .target("/v1/connection/native-port").request().get(Integer.class);
        assertEquals(TRANSPORT_PORT, port);
    }

    private static CassandraDaemonTask getMockTask(String name, String hostname, int nativePort) {
        CassandraDaemonTask mockTask = Mockito.mock(CassandraDaemonTask.class);
        when(mockTask.getState()).thenReturn(TaskState.TASK_RUNNING);
        when(mockTask.getName()).thenReturn(name);
        when(mockTask.getHostname()).thenReturn(hostname);
        CassandraConfig cassConf = Mockito.mock(CassandraConfig.class);
        when(mockTask.getConfig()).thenReturn(cassConf);
        CassandraApplicationConfig appConf = Mockito.mock(CassandraApplicationConfig.class);
        when(cassConf.getApplication()).thenReturn(appConf);
        when(appConf.getNativeTransportPort()).thenReturn(nativePort);
        return mockTask;
    }

    private ConnectionResource getConnectionResource(boolean withVips) {
        ConfigurationManager mockConfigManager = Mockito.mock(ConfigurationManager.class);
        CassandraSchedulerConfiguration mockSchedulerConfig = Mockito.mock(CassandraSchedulerConfiguration.class);
        CassandraConfig mockCassandraconfig = Mockito.mock(CassandraConfig.class);
        CassandraApplicationConfig mockAppConfig = Mockito.mock(CassandraApplicationConfig.class);
        try {
            when(mockConfigManager.getTargetConfig()).thenReturn(mockSchedulerConfig);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        when(mockSchedulerConfig.getCassandraConfig()).thenReturn(mockCassandraconfig);
        when(mockCassandraconfig.getApplication()).thenReturn(mockAppConfig);
        when(mockAppConfig.getNativeTransportPort()).thenReturn(TRANSPORT_PORT);

        ServiceConfig mockServiceConfig = Mockito.mock(ServiceConfig.class);
        when(mockSchedulerConfig.getServiceConfig()).thenReturn(mockServiceConfig);
        when(mockServiceConfig.getName()).thenReturn(TEST_SERVICE_NAME);

        CassandraState mockTasks = Mockito.mock(CassandraState.class);
        when(mockTasks.getDaemons()).thenReturn(TEST_TASKS);

        Capabilities mockCapabilities = Mockito.mock(Capabilities.class);
        try {
            when(mockCapabilities.supportsNamedVips()).thenReturn(withVips);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return new ConnectionResource(mockCapabilities, mockTasks, mockConfigManager);
    }
}