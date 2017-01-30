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
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to an S3 bucket.
 */
public class S3StorageDriver implements BackupStorageDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            S3StorageDriver.class);

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

    private TransferManager getS3TransferManager(BackupRestoreContext ctx) {
        final String accessKey = ctx.getAccountId();
        final String secretKey = ctx.getSecretKey();

        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        TransferManager tx = new TransferManager(basicAWSCredentials);
        return tx;
    }

    private File[] getNonSystemKeyspaces(BackupRestoreContext ctx) {
        File file = new File(ctx.getLocalLocation());
        File[] directories = file.listFiles(
                (current, name) -> new File(current, name).isDirectory() &&
                                   name.compareTo("system") != 0);
        return directories;
    }

    private static File[] getColumnFamilyDir(File keyspace) {
        return keyspace.listFiles(
                (current, name) -> new File(current, name).isDirectory());
    }

    @Override
    public void upload(BackupRestoreContext ctx) throws Exception {
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();
        final String key = getPrefixKey(ctx) + "/" + nodeId;
        LOGGER.info("Backup key: " + key);
        final TransferManager tx = getS3TransferManager(ctx);
        final File dataDirectory = new File(localLocation);

        try {
            // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
            for (File keyspaceDir : dataDirectory.listFiles()) {
                if (keyspaceDir.isFile()) {
                    // Skip any files in the data directory.
                    // Only enter keyspace directory.
                    continue;
                }
                LOGGER.info("Entering keyspace: {}", keyspaceDir.getName());
                for (File cfDir : getColumnFamilyDir(keyspaceDir)) {
                    LOGGER.info("Entering column family dir: {}", cfDir.getName());
                    File snapshotDir = new File(cfDir, "snapshots");
                    File backupDir = new File(snapshotDir, backupName);
                    if (!StorageUtil.isValidBackupDir(keyspaceDir, cfDir, snapshotDir, backupDir)) {
                        LOGGER.info("Skipping directory: {}",
                                snapshotDir.getAbsolutePath());
                        continue;
                    }
                    LOGGER.info(
                            "Valid backup directories. KeyspaceDir: {} | ColumnFamilyDir: {} | SnapshotDir: {} | BackupName: {}",
                            keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(),
                            snapshotDir.getAbsolutePath(), backupName);

                    final Optional<File> snapshotDirectory = StorageUtil.getValidSnapshotDirectory(
                            snapshotDir, backupName);
                    LOGGER.info("Valid snapshot directory: {}",
                            snapshotDirectory.isPresent());
                    if (snapshotDirectory.isPresent()) {
                        // Upload this directory
                        LOGGER.info("Going to upload directory: {}",
                                snapshotDirectory.get().getAbsolutePath());

                        uploadDirectory(
                                tx,
                                getBucketName(ctx),
                                key,
                                keyspaceDir.getName(),
                                cfDir.getName(),
                                snapshotDirectory.get());
                    } else {
                        LOGGER.warn(
                                "Snapshots directory: {} doesn't contain the current backup directory: {}",
                                snapshotDir.getName(), backupName);
                    }
                }
            }
            LOGGER.info("Done uploading snapshots for backup: {}", backupName);
        } catch (Exception e) {
            LOGGER.info("Failed uploading snapshots for backup: {}, error: {}", backupName, e);
            throw new Exception(e);
        } finally {
            tx.shutdownNow();
        }
    }

    private void uploadDirectory(TransferManager tx,
                                 String bucketName,
                                 String key,
                                 String keyspaceName,
                                 String cfName,
                                 File snapshotDirectory) throws Exception {
        try {
            final String fileKey = key + "/" + keyspaceName + "/" + cfName + "/";
            final MultipleFileUpload myUpload = tx.uploadDirectory(bucketName, fileKey, snapshotDirectory, true);
            myUpload.waitForCompletion();
        } catch (Exception e) {
            LOGGER.error("Error occurred on uploading directory {} : {}", snapshotDirectory.getName(), e);
            throw new Exception(e);
        }
    }

    @Override
    public void uploadSchema(BackupRestoreContext ctx, String schema) throws Exception {
        final String nodeId = ctx.getNodeId();
        final AmazonS3Client amazonS3Client = getAmazonS3Client(ctx);
        final String key = getPrefixKey(ctx) + "/" + nodeId + "/" + StorageUtil.SCHEMA_FILE;
        final InputStream stream = new ByteArrayInputStream(schema.getBytes(StandardCharsets.UTF_8));

        amazonS3Client.putObject(getBucketName(ctx), key, stream, new ObjectMetadata());
    }

    @Override
    public void download(BackupRestoreContext ctx) throws Exception {
        // download sstables at data/keyspace/cf/<files>
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();
        final File[] keyspaces = getNonSystemKeyspaces(ctx);
        final String bucketName = getBucketName(ctx);
        final String localLocation = ctx.getLocalLocation();
        final TransferManager tx = getS3TransferManager(ctx);
        final AmazonS3Client amazonS3Client = getAmazonS3Client(ctx);

        try {
            if (Objects.equals(ctx.getRestoreType(), new String("new"))) {
                final Map<String, Long> snapshotFileKeys = listSnapshotFiles(amazonS3Client,
                        bucketName,
                        backupName + File.separator + nodeId);
                LOGGER.info("Snapshot files for this node: {}", snapshotFileKeys);
                for (String fileKey : snapshotFileKeys.keySet()) {
                    downloadFile(tx, bucketName, fileKey, localLocation + File.separator + fileKey);
                }
            } else {
                for (File keyspace : keyspaces) {
                    for (File cfDir : getColumnFamilyDir(keyspace)) {
                        final String columnFamily = cfDir.getName().substring(0, cfDir.getName().indexOf("-"));
                        final Map<String, Long> snapshotFileKeys = listSnapshotFiles(amazonS3Client,
                                bucketName,
                                backupName + "/" + nodeId + "/" + keyspace.getName() + "/" + columnFamily);
                        for (String fileKey : snapshotFileKeys.keySet()) {
                            final String destinationFile = cfDir.getAbsolutePath() + fileKey.substring(fileKey.lastIndexOf("/"));
                            downloadFile(tx, bucketName, fileKey, destinationFile);
                            LOGGER.info("Keyspace {}, Column Family {}, FileKey {}, destination {}", keyspace, columnFamily, fileKey, destinationFile);
                        }
                    }
                }
            }
            LOGGER.info("Done downloading snapshots for backup: {}", backupName);
        } catch (Exception e) {
            LOGGER.info("Failed downloading snapshots for backup: {}, error: {}", backupName, e);
            throw new Exception(e);
        } finally {
            tx.shutdownNow();
        }
    }

    private void downloadFile(TransferManager tx,
                              String bucketName,
                              String sourcePrefixKey,
                              String destinationFile) throws Exception{
        try {
            final File snapshotFile = new File(destinationFile);
            // Only create parent directory once, if it doesn't exist.
            final File parentDir = new File(snapshotFile.getParent());
            if (!parentDir.isDirectory()) {
                final boolean parentDirCreated = parentDir.mkdirs();
                if (!parentDirCreated) {
                    LOGGER.error(
                            "Error creating parent directory for file: {}. Skipping to next",
                            destinationFile);
                    return;
                }
            }
            snapshotFile.createNewFile();
            final Download download = tx.download(bucketName, sourcePrefixKey, snapshotFile);
            download.waitForCompletion();
        } catch (Exception e) {
            LOGGER.error("Error downloading the file {} : {}", destinationFile, e);
            throw new Exception(e);
        }
    }

    @Override
    public String downloadSchema(BackupRestoreContext ctx) throws Exception {
        final String nodeId = ctx.getNodeId();
        final AmazonS3Client amazonS3Client = getAmazonS3Client(ctx);
        final String key = getPrefixKey(ctx) + "/" + nodeId + "/" + StorageUtil.SCHEMA_FILE;

        S3Object object = amazonS3Client.getObject(
                new GetObjectRequest(getBucketName(ctx), key));
        InputStream objectData = object.getObjectContent();
        String schema = IOUtils.toString(objectData, "UTF-8");
        objectData.close();
        return schema;
    }

    private static Map<String, Long> listSnapshotFiles(AmazonS3Client amazonS3Client,
                                                       String bucketName,
                                                       String backupName) {
        Map<String, Long> snapshotFiles = new HashMap<>();
        final ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(backupName);
        ListObjectsV2Result result;
        do {
            result = amazonS3Client.listObjectsV2(req);
            for (S3ObjectSummary objectSummary :
                    result.getObjectSummaries()) {
                snapshotFiles.put ( objectSummary.getKey ( ), objectSummary.getSize());
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated());

        return snapshotFiles;
    }
}
