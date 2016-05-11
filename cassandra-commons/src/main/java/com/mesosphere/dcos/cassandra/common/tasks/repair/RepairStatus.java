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
package com.mesosphere.dcos.cassandra.common.tasks.repair;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

import java.util.Optional;

/**
 * RepairStatus extends CassandraTaskStatus to implement the status Object for
 * RepairTask.
 */
public class RepairStatus extends CassandraTaskStatus {

    /**
     * Creates a RepairStatus.
     * @param state      The state of the task
     * @param id         The id of the task associated with the status.
     * @param slaveId    The id of the slave on which the task associated
     *                   with the status was launched.
     * @param executorId The id of the executor for the task associated with
     *                   the status.
     * @param message    An optional message sent from the executor.
     * @return A RepairStatus constructed from the parameters.
     */
    @JsonCreator
    public static RepairStatus create(
            @JsonProperty("state") Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("executor_id") String executorId,
            @JsonProperty("message") Optional<String> message) {
        return new RepairStatus(state, id, slaveId, executorId, message);
    }

    /**
     * Constructs a RepairStatus.
     * @param state      The state of the task
     * @param id         The id of the task associated with the status.
     * @param slaveId    The id of the slave on which the task associated
     *                   with the status was launched.
     * @param executorId The id of the executor for the task associated with
     *                   the status.
     * @param message    An optional message sent from the executor.
     */
    protected RepairStatus(Protos.TaskState state,
                           String id,
                           String slaveId,
                           String executorId,
                           Optional<String> message) {
        super(CassandraTask.TYPE.REPAIR,
                state,
                id,
                slaveId,
                executorId,
                message);
    }

    @Override
    public RepairStatus update(Protos.TaskState state) {
        if (isFinished()) {
            return this;
        } else {
            return create(state, id, slaveId, executorId, message);
        }
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        return CassandraProtos.CassandraTaskStatusData.newBuilder()
                .setType(CassandraProtos.CassandraTaskData.TYPE.REPAIR)
                .build();
    }
}
