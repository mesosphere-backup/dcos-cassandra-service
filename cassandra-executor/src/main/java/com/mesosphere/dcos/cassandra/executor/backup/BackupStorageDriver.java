package com.mesosphere.dcos.cassandra.executor.backup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BackupStorageDriver {
    void upload(BackupContext ctx) throws IOException;

    void download(RestoreContext ctx) throws IOException;

    Set<String> SKIP_KEYSPACES = ImmutableSet.of("system");

    Map<String, List<String>> SKIP_COLUMN_FAMILIES = ImmutableMap.of();
}
