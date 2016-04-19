/*
 * Copyright 2015 Mesosphere
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

package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;

import java.util.Objects;

public class DaemonInfo {

    public static DaemonInfo create(final CassandraDaemonTask task) {
        return new DaemonInfo(task);
    }

    @JsonProperty("name")
    private final String name;
    @JsonProperty("id")
    private final String id;
    @JsonProperty("hostname")
    private final String hostname;
    @JsonProperty("slave_id")
    private final String slaveId;
    @JsonProperty("state")
    private final Protos.TaskState state;
    @JsonProperty("operating_mode")
    private final CassandraMode mode;

    public DaemonInfo(final CassandraDaemonTask task) {
        name = task.getName();
        id = task.getId();
        hostname = task.getHostname();
        slaveId = task.getSlaveId();
        state = task.getStatus().getState();
        mode = task.getStatus().getMode();
    }

    public String getHostname() {
        return hostname;
    }

    public String getId() {
        return id;
    }

    public CassandraMode getMode() {
        return mode;
    }

    public String getName() {
        return name;
    }

    public String getSlaveId() {
        return slaveId;
    }

    public Protos.TaskState getState() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DaemonInfo)) return false;
        DaemonInfo that = (DaemonInfo) o;
        return Objects.equals(getName(), that.getName()) &&
                Objects.equals(getId(), that.getId()) &&
                Objects.equals(getHostname(), that.getHostname()) &&
                Objects.equals(getSlaveId(), that.getSlaveId()) &&
                getState() == that.getState() &&
                getMode() == that.getMode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getId(), getHostname(), getSlaveId(),
                getState(), getMode());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
