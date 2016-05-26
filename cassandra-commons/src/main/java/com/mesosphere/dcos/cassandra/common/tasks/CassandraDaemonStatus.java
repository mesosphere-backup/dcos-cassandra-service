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
    /**
     * Creates a CassandraDaemonStatus
     *
     * @param status The TaskStatus for the CassandraDaemon.
     * @return A CassandraDaemonStatus constructed from the parameters.
     */
    @JsonCreator
    public static CassandraDaemonStatus create(
        final Protos.TaskStatus status) {
        return new CassandraDaemonStatus(status);
    }

    /**
     * Constructs a CassandraDaemonStatus
     *
     * @param status The TaskStatus corresponding to the CassandraDaemon.
     */
    protected CassandraDaemonStatus(final Protos.TaskStatus status) {
        super(status);
    }

    /**
     * Gets the mode of the Cassandra daemon.
     *
     * @return The mode of the Cassandra daemon associated with the status.
     */
    public CassandraMode getMode() {
        return getData().getMode();
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
