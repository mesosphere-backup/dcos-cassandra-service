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
import java.util.stream.Collectors;

/**
 */
public final class StorageUtil {

  private static final Logger logger = LoggerFactory.getLogger(StorageUtil.class);
  private static final Set<String> SKIP_KEYSPACES = ImmutableSet.of("system");
  private static final Map<String, List<String>> SKIP_COLUMN_FAMILIES = ImmutableMap.of();
  private static final Set<String> SKIP_SYSTEM_KEYSPACES = ImmutableSet.of("system", "system_distributed", "system_traces", "system_schema", "system_auth");
  public static final String SCHEMA_FILE = "schema.cql";

  /**
   * Filters unwanted keyspaces and column families
   */
  static boolean isValidBackupDir(File ksDir, File cfDir, File ssDir, File bkDir) {
    if (!ssDir.exists() || !ssDir.isDirectory() || !bkDir.exists() || !bkDir.isDirectory()) {
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

  static Optional<File> getValidSnapshotDirectory(File snapshotsDir, String snapshotName) {
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

  public static List<String> filterSystemKeyspaces(List<String> keyspaces) {
    return keyspaces.stream()
            .filter(k -> !SKIP_SYSTEM_KEYSPACES.contains(k))
            .collect(Collectors.toList());
  }
}
