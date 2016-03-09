package com.mesosphere.dcos.cassandra.common.util;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import org.apache.mesos.Protos;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TaskUtils {
    private static final Set<Protos.TaskState> TERMINAL_STATES = new HashSet<>(Arrays.asList(
            Protos.TaskState.TASK_ERROR,
            Protos.TaskState.TASK_FAILED,
            Protos.TaskState.TASK_FINISHED,
            Protos.TaskState.TASK_KILLED,
            Protos.TaskState.TASK_LOST));

    private TaskUtils(){}

    public static int taskIdToNodeId(String taskId) {
        final String prefix = taskId.substring(0, taskId.indexOf('_'));
        final String idString = prefix.substring(prefix.lastIndexOf('-') + 1);
        return Integer.parseInt(idString);
    }

    public static int taskNameToNodeId(String taskId) {
        final int start = taskId.lastIndexOf("-");
        return Integer.parseInt(taskId.substring(start + 1));
    }

    public static boolean isTerminated(Protos.TaskState state) {
        return TERMINAL_STATES.contains(state);
    }

    public static boolean needsRecheduling(CassandraTask task) {
        final Protos.TaskState state = task.getStatus().getState();
        final CassandraTask.TYPE type = task.getType();

        if (state == Protos.TaskState.TASK_FINISHED && type != CassandraTask.TYPE.CASSANDRA_DAEMON) {
            return false;
        }

        return isTerminated(state);
    }
}
