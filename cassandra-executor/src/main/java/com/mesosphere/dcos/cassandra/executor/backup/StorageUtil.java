package com.mesosphere.dcos.cassandra.executor.backup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 */
public class StorageUtil {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Set<String> SKIP_KEYSPACES = ImmutableSet.of("system");
  private final Map<String, List<String>> SKIP_COLUMN_FAMILIES = ImmutableMap.of();

  /**
   * Filters unwanted keyspaces and column families
   */
  boolean isValidBackupDir(File ksDir, File cfDir, File bkDir) {
    if (!bkDir.isDirectory() && !bkDir.exists()) {
      return false;
    }

    String ksName = ksDir.getName();
    if (SKIP_KEYSPACES.contains(ksName)) {
      logger.debug("Skipping keyspace {}", ksName);
      return false;
    }

    String cfName = cfDir.getName();
    if (SKIP_COLUMN_FAMILIES.containsKey(ksName)
      && SKIP_COLUMN_FAMILIES.get(ksName).contains(cfName)) {
      logger.debug("Skipping column family: {}", cfName);
      return false;
    }

    return true;
  }

  Optional<File> getValidSnapshotDirectory(File snapshotsDir, String snapshotName) {
    File validSnapshot = null;
    for (File snapshotDir : snapshotsDir.listFiles())
      if (snapshotDir.getName().matches(snapshotName)) {
        // Found requested snapshot directory
        validSnapshot = snapshotDir;
        break;
      }

    // Requested snapshot directory not found
    return Optional.of(validSnapshot);
  }

  static boolean isAzure(String externalLocation) {
    // default to s3 (backward compatible)
    return StringUtils.isNotEmpty(externalLocation) && externalLocation.startsWith("azure:");
  }
}
