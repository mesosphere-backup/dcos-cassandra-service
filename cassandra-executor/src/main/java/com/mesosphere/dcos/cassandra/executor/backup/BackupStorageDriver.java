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
package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;

import java.io.IOException;

/**
 * BackupStorageDriver is the interface to all drivers that store and
 * retrieve snapshots for Cassandra backup.
 */
public interface BackupStorageDriver {

    /**
     * Uploads snapshot files to a remote location.
     * @param ctx The context of the backup.
     * @throws IOException If the upload fails.
     */
    void upload(BackupRestoreContext ctx) throws Exception;

    /**
     * Downloads snapshot files from a remote location.
     * @param ctx The context of the restore.
     * @throws IOException If the download from the remote location fails.
     */
    void download(BackupRestoreContext ctx) throws Exception;

    /**
     * Uploads the schema for current cassandra daemon process
     * @param ctx The context for the backup.
     * @param schema Schema for the current cassandra node.
     * @throws Exception If the upload fails.
     */
    void uploadSchema(BackupRestoreContext ctx, String schema) throws Exception;
}
