package com.mesosphere.dcos.cassandra.common.util;

import org.apache.mesos.Protos;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TaskUtils {
    private static final Set<Protos.TaskState> TERMINAL_STATES = new HashSet<>(
            Arrays.asList(
                    Protos.TaskState.TASK_ERROR,
                    Protos.TaskState.TASK_FAILED,
                    Protos.TaskState.TASK_FINISHED,
                    Protos.TaskState.TASK_KILLED,
                    Protos.TaskState.TASK_LOST));

    private TaskUtils() {
    }

    public static boolean isTerminated(Protos.TaskState state) {
        return TERMINAL_STATES.contains(state);
    }

}
