package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;

import java.io.IOException;

public interface BackupStorageDriver {
    void upload(BackupContext ctx) throws IOException;

    void download(RestoreContext ctx) throws IOException;
}
