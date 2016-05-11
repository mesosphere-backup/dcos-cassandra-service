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
package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import org.apache.mesos.Protos;

import java.util.Objects;
import java.util.Optional;


/**
 * CassandraDaemonStatus extends CassandraTaskStatus to implement the status
 * object for CassandraDeamonTask. It carries the mode of the Cassandra
 * daemon in addition to the basic status properties.
 */
public class CassandraDaemonStatus extends CassandraTaskStatus {

    @JsonProperty("mode")
    private final CassandraMode mode;

    /**
     * Creates a CassandraDaemonStatus
     * @param state      The state of the task
     * @param id         The id of the task associated with the status.
     * @param slaveId    The id of the slave on which the task associated
     *                   with the status was launched.
     * @param executorId The id of the executor for the task associated with
     *                   the status.
     * @param message    An optional message sent from the executor.
     * @param mode       The mode of the Cassandra daemon.
     * @return A CassandraDaemonStatus constructed from the parameters.
     */
    @JsonCreator
    public static CassandraDaemonStatus create(
            @JsonProperty("state") Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("executor_id") String executorId,
            @JsonProperty("message") Optional<String> message,
            @JsonProperty("mode") CassandraMode mode
    ) {
        return new CassandraDaemonStatus(state,
                id,
                slaveId,
                executorId,
                message,
                mode);
    }

    /**
     * Constructs a CassandraDaemonStatus
     * @param state      The state of the Cassandra daemon.
     * @param id         The id of the Cassandra daemon.
     * @param slaveId    The id of the slave on which the Cassandra daemon is
     *                   running.
     * @param executorId The id of the executor for the Cassandra daemon.
     * @param message    An optional message sent from the executor.
     * @param mode       The mode of the Cassandra daemon.
     */
    public CassandraDaemonStatus(final Protos.TaskState state,
                                 final String id,
                                 final String slaveId,
                                 final String executorId,
                                 final Optional<String> message,
                                 final CassandraMode mode) {
        super(CassandraTask.TYPE.CASSANDRA_DAEMON,
                state,
                id,
                slaveId,
                executorId,
                message);
        this.mode = mode;
    }

    /**
     * Gets the mode of the Cassandra daemon.
     * @return The mode of the Cassandra daemon associated with the status.
     */
    public CassandraMode getMode() {
        return mode;
    }


    @Override
    public CassandraDaemonStatus update(Protos.TaskState state) {
        return create(state, id, slaveId, executorId, message, mode);
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        CassandraProtos.CassandraTaskStatusData.Builder builder =
                CassandraProtos.CassandraTaskStatusData.newBuilder()
                        .setType(CassandraProtos.CassandraTaskData.TYPE
                                .CASSANDRA_DAEMON)
                        .setMode(mode.ordinal());

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraDaemonStatus)) return false;
        if (!super.equals(o)) return false;
        CassandraDaemonStatus that = (CassandraDaemonStatus) o;
        return Objects.equals(getMode(), that.getMode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getMode());
    }

}
