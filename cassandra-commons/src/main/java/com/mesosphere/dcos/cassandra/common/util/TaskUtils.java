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

    public static int taskIdToNodeId(String taskId) {
        final int start = taskId.indexOf("-");
        final int end = taskId.indexOf("_");
        return Integer.parseInt(taskId.substring(start + 1, end));
    }

    public static String getPersistenceId(Protos.TaskInfo taskInfo) {
        for (Protos.Resource resource : taskInfo.getResourcesList()) {
            if (resource.getName().equals("disk")) {
                Protos.Resource.DiskInfo diskInfo = resource.getDisk();
                if (diskInfo != null) {
                    Protos.Resource.DiskInfo.Persistence persistence = diskInfo.getPersistence();
                    return persistence.getId();
                }
            }
        }

        return null;
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
