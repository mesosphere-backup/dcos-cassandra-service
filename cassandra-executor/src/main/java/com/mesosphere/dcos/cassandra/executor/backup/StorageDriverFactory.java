package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the storage driver for uploading and downloading.  The external location should start
 * with "s3://xyz" or "azure://xyz".  The default is S3.
 */
public class StorageDriverFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageDriverFactory.class);

  public static BackupStorageDriver createStorageDriver(BackupUploadTask backupUploadTask) {
    final String externalLocation = backupUploadTask.getBackupContext().getExternalLocation();
    return getBackupStorageDriver(externalLocation);
  }

  public static BackupStorageDriver createStorageDriver(DownloadSnapshotTask downloadSnapshotTask) {
    final String externalLocation = downloadSnapshotTask.getRestoreContext().getExternalLocation();
    return getBackupStorageDriver(externalLocation);
  }

  private static BackupStorageDriver getBackupStorageDriver(String externalLocation) {
    // there is only 2 storage backends.  more and we should create a map of types.
    if (StorageUtil.isAzure(externalLocation)) {
      LOGGER.info("Using the Azure Driver.");
      return new AzureStorageDriver();
    } else {
      LOGGER.info("Using the S3 Driver.");
      return new S3StorageDriver();
    }
  }
}
