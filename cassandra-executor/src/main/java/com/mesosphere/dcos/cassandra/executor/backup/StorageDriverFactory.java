package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the storage driver for uploading and downloading.  The external location should start
 * with "s3://xyz" or "azure://xyz".  The default is S3.
 */
public class StorageDriverFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageDriverFactory.class);

  public static BackupStorageDriver createStorageDriver(CassandraTask cassandraTask) {

    String externalLocation = null;
    switch (cassandraTask.getType()) {
      case BACKUP_SNAPSHOT:
        externalLocation = ((BackupUploadTask)cassandraTask).getBackupRestoreContext().getExternalLocation();
        break;
      case BACKUP_SCHEMA:
        externalLocation = ((BackupSchemaTask)cassandraTask).getBackupRestoreContext().getExternalLocation();
        break;
    }
    return getBackupStorageDriver(externalLocation);
  }

  public static BackupStorageDriver createStorageDriver(DownloadSnapshotTask downloadSnapshotTask) {
    final String externalLocation = downloadSnapshotTask.getBackupRestoreContext().getExternalLocation();
    return getBackupStorageDriver(externalLocation);
  }

  private static BackupStorageDriver getBackupStorageDriver(String externalLocation) {
    if (StorageUtil.isAzure(externalLocation)) {
      LOGGER.info("Using the Azure Driver.");
      return new AzureStorageDriver();
    } else {
      LOGGER.info("Using the S3 Driver.");
      return new S3StorageDriver();
    }
  }
}
