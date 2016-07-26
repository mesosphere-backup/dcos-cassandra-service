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
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to an S3 bucket.
 */
public class S3StorageDriver implements BackupStorageDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            S3StorageDriver.class);

    public static final int DEFAULT_PART_SIZE_UPLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB
    public static final int DEFAULT_PART_SIZE_DOWNLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB

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
        final S3ClientOptions options = new S3ClientOptions();
        options.setPathStyleAccess(true);
        final AmazonS3Client amazonS3Client = new AmazonS3Client(basicAWSCredentials);
        amazonS3Client.setEndpoint(endpoint);
        amazonS3Client.setS3ClientOptions(options);
        return amazonS3Client;
    }

    @Override
    public void upload(BackupContext ctx) throws IOException, URISyntaxException {
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

    private boolean isValidFileForUpload(File file) {
        return file.isFile() && !file.getName().endsWith(".json");
    }

    private void uploadDirectory(String localLocation,
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
                        if (isValidFileForUpload(file)) {
                            String fileKey = key + "/" + keyspaceName + "/" + cfName + "/" + file.getName();
                            List<PartETag> partETags = new ArrayList<>();
                            final InitiateMultipartUploadRequest uploadRequest = new InitiateMultipartUploadRequest(bucketName, fileKey);
                            final InitiateMultipartUploadResult uploadResult = amazonS3Client.initiateMultipartUpload(uploadRequest);

                            LOGGER.info(
                                    "Initiating upload for file: {} | bucket: {} | key: {} | uploadId: {}",
                                    file.getAbsolutePath(), bucketName, fileKey,
                                    uploadResult.getUploadId());

                            final byte[] buffer = new byte[DEFAULT_PART_SIZE_UPLOAD];
                            BufferedInputStream inputStream = null;
                            ByteArrayOutputStream baos = null;
                            SnappyOutputStream compress = null;
                            try {
                                inputStream = new BufferedInputStream(new FileInputStream(file));
                                baos = new ByteArrayOutputStream();
                                compress = new SnappyOutputStream(baos);

                                int chunkLength;
                                int partNum = 1;
                                while ((chunkLength = inputStream.read(buffer)) != -1) {
                                    LOGGER.debug("Compressing part: {}", partNum);
                                    compress.write(buffer, 0, chunkLength);
                                    compress.flush();
                                    final byte[] bytes = baos.toByteArray();
                                    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                                    final MessageDigest md5Digester = MessageDigest.getInstance("MD5");
                                    final byte[] digest = md5Digester.digest(bytes);
                                    final String md5String = Base64.getEncoder().encodeToString(digest);

                                    final UploadPartRequest uploadPartRequest = new UploadPartRequest()
                                            .withBucketName(bucketName)
                                            .withKey(fileKey)
                                            .withUploadId(uploadResult.getUploadId())
                                            .withPartNumber(partNum++)
                                            .withInputStream(bais)
                                            .withPartSize(bytes.length)
                                            .withMD5Digest(md5String);

                                    LOGGER.debug(
                                            "Uploading part: {} | bucket: {} | key: {} | uploadId: {}",
                                            partNum, bucketName, fileKey,
                                            uploadResult.getUploadId());
                                    final UploadPartResult uploadPartResult = amazonS3Client.uploadPart(
                                            uploadPartRequest);
                                    final PartETag partETag = uploadPartResult.getPartETag();
                                    if (!partETag.getETag().equals(new String(
                                            Hex.encodeHex(digest)))) {
                                        LOGGER.error("Error matching hex");
                                        // TODO: Add retry logic
                                    }
                                    partETags.add(partETag);
                                }
                                LOGGER.debug(
                                        "Done uploading, now completing bucket: {} | key: {} | uploadId: {}",
                                        bucketName, fileKey,
                                        uploadResult.getUploadId());
                                final CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                                        bucketName, fileKey,
                                        uploadResult.getUploadId(), partETags);
                                amazonS3Client.completeMultipartUpload(completeMultipartUploadRequest);
                                LOGGER.debug(
                                        "Successfully uploaded the file to S3. Deleting the file now: {}",
                                        file.getAbsolutePath());
                                final boolean delete = file.delete();
                                LOGGER.debug("Deletion status: {} for file {}",
                                        delete, file.getAbsolutePath());
                            } catch (Exception e) {
                                LOGGER.error("Error uploading file: {}", e);
                                amazonS3Client.abortMultipartUpload(
                                        new AbortMultipartUploadRequest(
                                                bucketName, fileKey,
                                                uploadResult.getUploadId()));
                                // TODO: Add retry logic
                            } finally {
                                IOUtils.closeQuietly(inputStream);
                                IOUtils.closeQuietly(compress);
                                IOUtils.closeQuietly(baos);
                            }
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
    public void download(RestoreContext ctx) throws IOException, URISyntaxException {
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
                              Long sizeInBytes) {
        LOGGER.info(
                "DownloadFile | Local location: {} | Bucket Name: {} | fileKey: {} | Size in bytes: {}",
                localLocation, bucketName, fileKey, sizeInBytes);
        InputStream inputStream = null;
        BufferedOutputStream bos = null;
        SnappyInputStream is = null;
        FileOutputStream fileOutputStream = null;
        try {
            long position = 0;
            final String fileLocation = localLocation + File.separator + fileKey;
            File f = new File(fileLocation);

            // Only create parent directory once, if it doesn't exist.
            final File parentDir = new File(f.getParent());
            if (!parentDir.isDirectory()) {
                final boolean parentDirCreated = parentDir.mkdirs();
                if (!parentDirCreated) {
                    LOGGER.error(
                            "Error creating parent directory for file: {}. Skipping to next",
                            fileLocation);
                    return;
                }
            }

            fileOutputStream = new FileOutputStream(f, true);
            while (position <= sizeInBytes) {
                final byte[] buffer = new byte[DEFAULT_PART_SIZE_DOWNLOAD];
                final GetObjectRequest rangeObjectRequest = new GetObjectRequest(
                        bucketName, fileKey);

                rangeObjectRequest.setRange(position,
                        DEFAULT_PART_SIZE_DOWNLOAD);
                final S3Object object = amazonS3Client.getObject(
                        rangeObjectRequest);

                inputStream = object.getObjectContent();
                is = new SnappyInputStream(
                        new BufferedInputStream(inputStream));
                bos = new BufferedOutputStream(fileOutputStream,
                        DEFAULT_PART_SIZE_DOWNLOAD);
                int bytesWritten = 0;
                try {
                    int c;
                    while ((c = is.read(buffer, 0,
                            DEFAULT_PART_SIZE_DOWNLOAD)) != -1) {
                        bytesWritten += c;
                        bos.write(buffer, 0, c);
                    }
                } finally {
                    IOUtils.closeQuietly(is);
                    IOUtils.closeQuietly(bos);
                }

                // TODO: Think about resumable downloads in future.
                LOGGER.info("For file: {} wrote: {} bytes out of {} bytes",
                        fileKey, position + bytesWritten, sizeInBytes);
                position += DEFAULT_PART_SIZE_DOWNLOAD + 1;
            }
        } catch (Exception e) {
            LOGGER.error(
                    "Error occuring while downloading fileKey: {} from bucket: {}. Reason: ",
                    fileKey, bucketName, e);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(fileOutputStream);
        }
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
