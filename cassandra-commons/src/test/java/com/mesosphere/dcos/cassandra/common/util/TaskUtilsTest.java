package com.mesosphere.dcos.cassandra.common.util;

import org.junit.Assert;
import org.junit.Test;

public class TaskUtilsTest {
    @Test
    public void testTaskIdToNodeId() {
        Assert.assertEquals(0, TaskUtils.taskIdToNodeId("node-0_1asgasgsrg"));
        Assert.assertEquals(10, TaskUtils.taskIdToNodeId("node-10_1raweg"));
        Assert.assertEquals(100, TaskUtils.taskIdToNodeId("node-100_12rtawew5"));
    }

    @Test
    public void testTaskNameToNodeId() {
        Assert.assertEquals(0, TaskUtils.taskNameToNodeId("node-0"));
        Assert.assertEquals(10, TaskUtils.taskNameToNodeId("node-10"));
        Assert.assertEquals(100, TaskUtils.taskNameToNodeId("node-100"));
    }

    @Test(expected = NumberFormatException.class)
    public void testTaskNameToNodeIdFaulty() {
        Assert.assertEquals(0, TaskUtils.taskNameToNodeId("node-0_"));
    }
}
