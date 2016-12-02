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
package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

/**
 * BackupSnapshotStatus extends CassandraTaskStatus to implement the status
 * Object for the BackupSnapshot task.
 */
public class BackupSnapshotStatus extends CassandraTaskStatus {

    public static BackupSnapshotStatus create(final Protos.TaskStatus status) {
        return new BackupSnapshotStatus(status);
    }

    protected BackupSnapshotStatus(final Protos.TaskStatus status) {
        super(status);
    }
}
