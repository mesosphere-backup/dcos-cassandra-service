package com.mesosphere.dcos.cassandra.common.tasks.backup;

/**
 * Created by gabriel on 7/15/16.
 */
public interface BackupRestoreContext {
    String getLocalLocation();
    String getExternalLocation();
    String getAccountId();
    String getSecretKey();
    String getNodeId();
    String getName();
}
