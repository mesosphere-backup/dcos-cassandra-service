package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairStatus;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RepairTest {

    @Mock
    private ExecutorDriver executorDriver;

    @Mock
    private CassandraDaemonProcess cassandraDaemonProcess;

    @Mock
    private RepairTask repairTask;

    @Mock
    private RepairStatus repairStatus;

    private Repair repair;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        RepairContext repairContext = RepairContext.create(
                Collections.singletonList("node-1"),
                Collections.singletonList("my_keyspace"),
                Arrays.asList("table1", "table2"));
        when(repairTask.getRepairContext()).thenReturn(repairContext);
        when(repairTask.createStatus(any(Protos.TaskState.class), any(Optional.class))).thenReturn(repairStatus);
        repair = new Repair(executorDriver, cassandraDaemonProcess, repairTask);
    }

    @Test
    public void testRepairKeyspace() throws Exception {
        repair.run();

        ArgumentCaptor<Map> optionsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraDaemonProcess).repair(eq("my_keyspace"), optionsCaptor.capture());
        Map<String, String> repairOptions = optionsCaptor.getValue();

        assertEquals(repairOptions.get(RepairOption.PRIMARY_RANGE_KEY), "true");
        assertEquals(repairOptions.get(RepairOption.INCREMENTAL_KEY), "true");
        assertEquals(repairOptions.get(RepairOption.COLUMNFAMILIES_KEY), "table1,table2");
    }
}