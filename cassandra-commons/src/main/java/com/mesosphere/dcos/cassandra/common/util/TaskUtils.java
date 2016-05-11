/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.common.util;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import org.apache.mesos.Protos;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains static utility methods for dealing with tasks.
 */
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

    /**
     * Tests if the state is terminal.
     * @param state The state to test.
     * @return True if the state is terminal.
     */
    public static boolean isTerminated(Protos.TaskState state) {
        return TERMINAL_STATES.contains(state);
    }

    /**
     * Tests if the task is terminated.
     * @param task The task to test.
     * @return True if the task is in a terminal state.
     */
    public static boolean isTerminal(CassandraTask task){

        return isTerminated(task.getStatus().getState());
    }

}
