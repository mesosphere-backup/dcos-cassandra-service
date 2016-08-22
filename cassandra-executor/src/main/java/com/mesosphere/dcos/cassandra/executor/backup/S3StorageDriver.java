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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to an S3 bucket.
 */
public class S3StorageDriver implements BackupStorageDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            S3StorageDriver.class);
    private StorageUtil storageUtil = new StorageUtil();

    String getBucketName(BackupRestoreContext ctx) throws URISyntaxException {
        URI uri = new URI(ctx.getExternalLocation());
        LOGGER.info("URI: " + uri);
        if (uri.getScheme().equals(AmazonS3Client.S3_SERVICE_NAME)) {
            return uri.getHost();
        } else {
            return uri.getPath().split("/")[1];
        }
    }

    String getPrefixKey(BackupRestoreContext ctx) throws URISyntaxException {
        URI uri = new URI(ctx.getExternalLocation());
        String[] segments = uri.getPath().split("/");

        int startIndex = uri.getScheme().equals(AmazonS3Client.S3_SERVICE_NAME) ? 1 : 2;
        String prefixKey = "";
        for (int i=startIndex; i<segments.length; i++) {
            prefixKey += segments[i];
            if (i < segments.length - 1) {
                prefixKey += "/";
            }
        }

        prefixKey = (prefixKey.length() > 0 && !prefixKey.endsWith("/")) ? prefixKey + "/" : prefixKey;
        prefixKey += ctx.getName(); // append backup name

        return prefixKey;
    }

    String getEndpoint(BackupRestoreContext ctx) throws URISyntaxException {
        URI uri = new URI(ctx.getExternalLocation());
        String scheme = uri.getScheme();
        if (scheme.equals(AmazonS3Client.S3_SERVICE_NAME)) {
            return Constants.S3_HOSTNAME;
        } else {
            String endpoint = scheme + "://" + uri.getHost();

            int port = uri.getPort();
            if (port != -1) {
                endpoint += ":" + Integer.toString(port);
            }

            return endpoint;
        }
    }

    private AmazonS3Client getAmazonS3Client(BackupRestoreContext ctx) throws URISyntaxException {
        final String accessKey = ctx.getAccountId();
        final String secretKey = ctx.getSecretKey();
        String endpoint = getEndpoint(ctx);
        LOGGER.info("endpoint: {}", endpoint);

        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3Client amazonS3Client = new AmazonS3Client(basicAWSCredentials);
        amazonS3Client.setEndpoint(endpoint);

        if (ctx.usesEmc()) {
            final S3ClientOptions options = new S3ClientOptions();
            options.setPathStyleAccess(true);
            amazonS3Client.setS3ClientOptions(options);
        }

        return amazonS3Client;
    }

    @Override
    public void upload(BackupRestoreContext ctx) throws IOException, URISyntaxException {
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();

        final String key = getPrefixKey(ctx) + "/" + nodeId;
        LOGGER.info("Backup key: " + key);
        final AmazonS3Client amazonS3Client = getAmazonS3Client(ctx);

        final File dataDirectory = new File(localLocation);

        // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
        for (File keyspaceDir : dataDirectory.listFiles()) {
            if (keyspaceDir.isFile()) {
                // Skip any files in the data directory.
                // Only enter keyspace directory.
                continue;
            }
            LOGGER.info("Entering keyspace: {}", keyspaceDir.getName());
            for (File cfDir : keyspaceDir.listFiles()) {
                LOGGER.info("Entering column family: {}", cfDir.getName());
                File snapshotDir = new File(cfDir, "snapshots");
                if (!storageUtil.isValidBackupDir(keyspaceDir, cfDir, snapshotDir)) {
                    LOGGER.info("Skipping directory: {}",
                            snapshotDir.getAbsolutePath());
                    continue;
                }
                LOGGER.info(
                        "Valid backup directories. Keyspace: {} | ColumnFamily: {} | Snapshot: {} | BackupName: {}",
                        keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(),
                        snapshotDir.getAbsolutePath(), backupName);

                final Optional<File> snapshotDirectory = storageUtil.getValidSnapshotDirectory(
                         snapshotDir, backupName);
                LOGGER.info("Valid snapshot directory: {}",
                        snapshotDirectory.isPresent());
                if (snapshotDirectory.isPresent()) {
                    // Upload this directory
                    LOGGER.info("Going to upload directory: {}",
                            snapshotDirectory.get().getAbsolutePath());
                    uploadDirectory(
                            ctx,
                            snapshotDirectory.get().getAbsolutePath(),
                            amazonS3Client,
                            getBucketName(ctx),
                            key,
                            keyspaceDir.getName(),
                            cfDir.getName());
                } else {
                    LOGGER.warn(
                            "Snapshots directory: {} doesn't contain the current backup directory: {}",
                            snapshotDir.getName(), backupName);
                }
            }
        }

        LOGGER.info("Done uploading snapshots for backup: {}", backupName);
    }

    private boolean isValidFileForUpload(File file, BackupRestoreContext backupRestoreContext) {
        return file.isFile() && hasValidSuffix(file, backupRestoreContext);
    }

    private boolean hasValidSuffix(File file, BackupRestoreContext backupRestoreContext) {
        LOGGER.info("hasValidSuffix() BackupRestoreContext: " + backupRestoreContext);
        if (backupRestoreContext.usesEmc()) {
            return !file.getName().endsWith(".json");
        } else {
            return true;
        }
    }

    private void uploadDirectory(
            BackupRestoreContext BackupRestoreContext,
            String localLocation,
            AmazonS3Client amazonS3Client,
            String bucketName,
            String key,
            String keyspaceName,
            String cfName) throws IOException {
        LOGGER.info(
                "uploadDirectory() localLocation: {}, AmazonS3Client: {}, bucketName: {}, key: {}, keyspaceName: {}, cfName: {}",
                localLocation, amazonS3Client, bucketName, key, keyspaceName,
                cfName);

        Files.walkFileTree(FileSystems.getDefault().getPath(localLocation),
                new FileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir,
                                                             BasicFileAttributes attrs)
                            throws IOException {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                            throws IOException {
                        final File file = path.toFile();
                        LOGGER.info("Visiting file: {}", file.getAbsolutePath());
                        if (isValidFileForUpload(file, BackupRestoreContext)) {
                            String fileKey = key + "/" + keyspaceName + "/" + cfName + "/" + file.getName();

                            LOGGER.info(
                                    "Initiating upload for file: {} | bucket: {} | key: {}",
                                    file.getAbsolutePath(), bucketName, fileKey);

                            amazonS3Client.putObject(bucketName,fileKey,file);

                            LOGGER.debug(
                                        "Successfully uploaded the file to S3. Deleting the file now: {}",
                                        file.getAbsolutePath());
                            final boolean delete = file.delete();
                            LOGGER.debug("Deletion status: {} for file {}",
                                        delete, file.getAbsolutePath());

                        }

                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file,
                                                           IOException exc)
                            throws IOException {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir,
                                                              IOException exc)
                            throws IOException {
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    @Override
    public void download(BackupRestoreContext ctx) throws IOException, URISyntaxException {
        // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>

        // Location of data directory, where the data will be copied.
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();

        final String bucketName = getBucketName(ctx);
        final AmazonS3Client amazonS3Client = getAmazonS3Client(ctx);

        final Map<String, Long> snapshotFileKeys = listSnapshotFiles(amazonS3Client, bucketName, backupName + "/" + nodeId);

        LOGGER.info("Snapshot files for this node: {}", snapshotFileKeys);

        for (String fileKey : snapshotFileKeys.keySet()) {
            downloadFile(localLocation, bucketName, amazonS3Client, fileKey,
                    snapshotFileKeys.get(fileKey));
        }
    }

    private void downloadFile(String localLocation,
                              String bucketName,
                              AmazonS3Client amazonS3Client,
                              String fileKey,
                              Long sizeInBytes) throws IOException {
        LOGGER.info(
                "DownloadFile | Local location: {} | Bucket Name: {} | fileKey: {} | Size in bytes: {}",
                localLocation, bucketName, fileKey, sizeInBytes);

            final String fileLocation = localLocation + File.separator + fileKey;
            File file = new File(fileLocation);

            // Only create parent directory once, if it doesn't exist.
            final File parentDir = new File(file.getParent());
            if (!parentDir.isDirectory()) {
                final boolean parentDirCreated = parentDir.mkdirs();
                if (!parentDirCreated) {
                    LOGGER.error(
                            "Error creating parent directory for file: {}. Skipping to next",
                            fileLocation);
                    return;
                }
            }

            amazonS3Client.getObject(
              new GetObjectRequest(bucketName, fileKey),
              file);
    }

    public Map<String, Long> listSnapshotFiles(AmazonS3Client amazonS3Client,
                                               String bucketName,
                                               String backupName) {
        Map<String, Long> snapshotFiles = new HashMap<>();
        ObjectListing objectListing;
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(backupName);
        do {
            objectListing = amazonS3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                snapshotFiles.put(objectSummary.getKey(),
                        objectSummary.getSize());
            }
        } while (objectListing.isTruncated());

        return snapshotFiles;
    }
}
