package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to an Azure Storage using Page Blobs.
 * Page Blobs allow for 1TB file sizes.
 * Page Blobs require a Storage Account (but NOT a blob storage account)
 */
public class AzureStorageDriver implements BackupStorageDriver {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final int PAGE_BLOB_PAGE_SIZE = 512;
  private static final String File_SIZE_KEY = "size";

  private static final int DEFAULT_PART_SIZE_UPLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB
  private static final int DEFAULT_PART_SIZE_DOWNLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB

  private StorageUtil storageUtil = new StorageUtil();

  @Override
  public void upload(BackupContext ctx) throws IOException {

    final String accountName = ctx.getAcccountId();
    final String accountKey = ctx.getSecretKey();
    final String localLocation = ctx.getLocalLocation();
    final String backupName = ctx.getName();
    final String nodeId = ctx.getNodeId();

    final String key = String.format("%s/%s", backupName, nodeId);
    final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
    // https://<account_name>.blob.core.windows.net/<container_name>
    final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

    final File dataDirectory = new File(localLocation);
    if (container == null || !dataDirectory.isDirectory()) {
      logger.error("Error uploading snapshots.  Unable to connect to {}, for container {} or Directory {} doesn't exist.",
        ctx.getExternalLocation(), containerName, localLocation);
      return;
    }

    // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
    for (File keyspaceDir : dataDirectory.listFiles()) {
      if (keyspaceDir.isFile()) {
        // Skip any files in the data directory.
        // Only enter keyspace directory.
        continue;
      }
      logger.info("Entering keyspace: {}", keyspaceDir.getName());
      for (File cfDir : keyspaceDir.listFiles()) {
        logger.info("Entering column family: {}", cfDir.getName());
        File snapshotDir = new File(cfDir, "snapshots");
        if (!storageUtil.isValidBackupDir(keyspaceDir, cfDir, snapshotDir)) {
          logger.info("Skipping directory: {}", snapshotDir.getAbsolutePath());
          continue;
        }
        logger.info(
          "Valid backup directories. Keyspace: {} | ColumnFamily: {} | Snapshot: {} | BackupName: {}",
          keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(),
          snapshotDir.getAbsolutePath(), backupName);

        final Optional<File> snapshotDirectory = storageUtil.getValidSnapshotDirectory(snapshotDir, backupName);
        logger.info("Valid snapshot directory: {}", snapshotDirectory.isPresent());

        if (snapshotDirectory.isPresent()) {
          logger.info("Going to upload directory: {}", snapshotDirectory.get().getAbsolutePath());

          uploadDirectory(snapshotDirectory.get().getAbsolutePath(), container, containerName, key,
            keyspaceDir.getName(), cfDir.getName());

        } else {
          logger.warn(
            "Snapshots directory: {} doesn't contain the current backup directory: {}",
            snapshotDir.getName(), backupName);
        }
      }
    }

    logger.info("Done uploading snapshots for backup: {}", backupName);
  }

  private void uploadDirectory(String localLocation,
    CloudBlobContainer azureContainer,
    String containerName,
    String key,
    String keyspaceName,
    String cfName) throws IOException {

    logger.info(
      "uploadDirectory() localLocation: {}, containerName: {}, key: {}, keyspaceName: {}, cfName: {}",
      localLocation, containerName, key, keyspaceName, cfName);

    Files.walk(FileSystems.getDefault().getPath(localLocation)).forEach(filePath -> {
        File file = filePath.toFile();
        if (file.isFile()) {
          String fileKey = key + "/" + keyspaceName + "/" + cfName + "/" + file.getName();
          uploadFile(azureContainer, fileKey, file);
        }
      }
    );
  }

  private void uploadFile(CloudBlobContainer container, String fileKey, File sourceFile) {

    BlobOutputStream blobOutputStream = null;
    try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(sourceFile))) {

      logger.info("Initiating upload for file: {} | key: {}",
        sourceFile.getAbsolutePath(), fileKey);

      final CloudPageBlob blob = container.getPageBlobReference(fileKey);
      long sourceFileOriginalLength = sourceFile.length();
      long sourceFilePageSized = roundToPageBlobSize(sourceFileOriginalLength);

      int buffSize = DEFAULT_PART_SIZE_UPLOAD;
      if (buffSize > sourceFilePageSized) {
        logger.debug("Buffer size downgraded to rounded source length of {}", sourceFilePageSized);
        buffSize = (int) sourceFilePageSized;
      }
      byte[] buffer = new byte[buffSize];

      blobOutputStream = blob.openWriteNew(sourceFilePageSized);
      long leftToWrite = sourceFilePageSized;
      while ((inputStream.read(buffer)) != -1) {
        // normally we would use the bytes read, however we need to page align
        int writeCount = (int) Math.min(buffSize, leftToWrite);
        leftToWrite -= writeCount;

        // the writecount could be greater than what was read but the buffer is initialized with 0 (perfect!)
        blobOutputStream.write(buffer, 0, writeCount);
        buffer = new byte[buffSize];
      }

      logger.info("For file {} wrote: {} bytes for original file size of {}",
        fileKey, sourceFilePageSized, sourceFileOriginalLength);

      // file size on azure will be page sized.  we need to return to the actual file size... metadata FTW!
      blob.setMetadata(fileMetaData(sourceFileOriginalLength));
      blob.uploadMetadata();

    } catch (StorageException | URISyntaxException | IOException e) {
      logger.error("Unable to store blob", e);
    } finally {
      IOUtils.closeQuietly(blobOutputStream);
    }
  }

  @Override
  public void download(RestoreContext ctx) throws IOException {

    final String accountName = ctx.getAcccountId();
    final String accountKey = ctx.getSecretKey();
    final String localLocation = ctx.getLocalLocation();
    final String backupName = ctx.getName();
    final String nodeId = ctx.getNodeId();

    final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
    // https://<account_name>.blob.core.windows.net/<container_name>
    final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

    if (container == null) {
      logger.error("Error uploading snapshots.  Unable to connect to {}, for container {}.",
        ctx.getExternalLocation(), containerName, localLocation);
      return;
    }
    String keyPrefix = String.format("%s/%s", backupName, nodeId);

    final Map<String, Long> snapshotFileKeys = getSnapshotFileKeys(container, keyPrefix);
    logger.info("Snapshot files for this node: {}", snapshotFileKeys);

    for (String fileKey : snapshotFileKeys.keySet()) {
      downloadFile(localLocation, container, fileKey, snapshotFileKeys.get(fileKey));
    }
  }

  private void downloadFile(String localLocation, CloudBlobContainer container, String fileKey, long originalSize) {

    logger.info("Downloading |  Local location {} | fileKey: {} | Size: {}", localLocation, fileKey, originalSize);

    InputStream inputStream = null;
    final String fileLocation = localLocation + File.separator + fileKey;
    File file = new File(fileLocation);
    // Only create parent directory once, if it doesn't exist.
    if (!createParentDir(file)) {
      logger.error("Unable to create parent directories!");
      return;
    }

    try (
      FileOutputStream fileOutputStream = new FileOutputStream(file, true);
      BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream)) {

      final CloudPageBlob pageBlobReference = container.getPageBlobReference(fileKey);
      inputStream = pageBlobReference.openInputStream();

      int buffSize = DEFAULT_PART_SIZE_DOWNLOAD;
      if (buffSize > originalSize) {
        logger.debug("Buffer size downgraded to rounded source length of {}", originalSize);
        buffSize = (int) originalSize;
      }

      final byte[] buffer = new byte[buffSize];
      long leftToWrite = originalSize;
      while ((inputStream.read(buffer, 0, buffSize)) != -1) {
        int writeCount = (int) Math.min(buffSize, leftToWrite);
        leftToWrite -= writeCount;
        bos.write(buffer, 0, writeCount);
      }
    } catch (Exception e) {
      logger.error("Unable to write file: {}", fileKey, e);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
  }

  private String getContainerName(String externalLocation) {
    return externalLocation.substring("azure://".length()).replace("/", "");
  }

  private CloudBlobContainer getCloudBlobContainer(String accountName, String accountKey, String containerName) {
    CloudBlobContainer container = null;

    if (StringUtils.isNotBlank(containerName)) {
      final String storageConnectionString = "DefaultEndpointsProtocol=http"
        + ";AccountName=" + accountName
        + ";AccountKey=" + accountKey;

      try {
        final CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
        CloudBlobClient serviceClient = account.createCloudBlobClient();

        container = serviceClient.getContainerReference(containerName);
        container.createIfNotExists();
      } catch (StorageException | URISyntaxException | InvalidKeyException e) {
        logger.error("Error connecting to container for account {} and container name {}", accountName, containerName, e);
      }
    }

    return container;
  }

  private boolean createParentDir(File file) {
    final File parentDir = new File(file.getParent());
    if (!parentDir.isDirectory()) {
      final boolean parentDirCreated = parentDir.mkdirs();
      if (!parentDirCreated) {
        logger.error("Error creating parent directory for file: {}. Skipping to next");
        return false;
      }
    }
    return true;
  }

  private static long roundToPageBlobSize(long size) {
    return (size + PAGE_BLOB_PAGE_SIZE - 1) & ~(PAGE_BLOB_PAGE_SIZE - 1);
  }

  private HashMap<String, String> fileMetaData(long sourceFileOriginalLength) {
    HashMap<String, String> metadata = new HashMap<>(1);
    metadata.put(File_SIZE_KEY, sourceFileOriginalLength + "");
    return metadata;
  }

  private Map<String, Long> getSnapshotFileKeys(CloudBlobContainer container, String keyPrefix) {
    Map<String, Long> snapshotFiles = new HashMap<>();

    try {
      for (ListBlobItem item : container.listBlobs(keyPrefix, true)) {
        if (item instanceof CloudPageBlob) {
          CloudPageBlob cloudBlob = (CloudPageBlob) item;
          snapshotFiles.put(cloudBlob.getName(), getOriginalFileSize(cloudBlob));
        }
      }
    } catch (StorageException e) {
      logger.error("Unable to retrieve metadata.", e);
      // all or none
      snapshotFiles = new HashMap<>();
    }
    return snapshotFiles;
  }

  private long getOriginalFileSize(CloudPageBlob pageBlobReference) throws StorageException {
    long size = 0;

    pageBlobReference.downloadAttributes();
    HashMap<String, String> map = pageBlobReference.getMetadata();
    if (map != null && map.size() > 0) {
      try {
        size = Long.parseLong(map.get(File_SIZE_KEY));
      } catch (Exception e) {
        logger.error("File size metadata missing or is not a number.");
      }
    }

    return size;
  }
}
