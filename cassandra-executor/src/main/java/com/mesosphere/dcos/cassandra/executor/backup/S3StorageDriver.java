package com.mesosphere.dcos.cassandra.executor.backup;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.backup.RestoreContext;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.*;

public class S3StorageDriver implements BackupStorageDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageDriver.class);

    protected final Set<String> SKIP_KEYSPACES = ImmutableSet.of("system");
    protected final Map<String, List<String>> SKIP_COLUMN_FAMILIES = ImmutableMap.of();

    public static final int DEFAULT_PART_SIZE = 4 * 1024 * 1024; // Chunk size set to 4MB

    @Override
    public void upload(BackupContext ctx) throws IOException {
        final String accessKey = ctx.getS3AccessKey();
        final String secretKey = ctx.getS3SecretKey();
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();

        final AmazonS3URI backupLocationURI = new AmazonS3URI(ctx.getExternalLocation());
        final String bucketName = backupLocationURI.getBucket();

        String prefixKey = backupLocationURI.getKey() != null ? backupLocationURI.getKey() : "";
        prefixKey = (prefixKey.length() > 0 && !prefixKey.endsWith("/")) ? prefixKey + "/" : prefixKey;
        prefixKey += backupName;
        final String key = prefixKey;

        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3Client amazonS3Client = new AmazonS3Client(basicAWSCredentials);

        final File dataDirectory = new File(localLocation);
        final String dataDirAbsolutePath = dataDirectory.getAbsolutePath();

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
                if (!isValidBackupDir(keyspaceDir, cfDir, snapshotDir)) {
                    LOGGER.info("Skipping directory: {}", snapshotDir.getAbsolutePath());
                    continue;
                }
                LOGGER.info("Valid backup directories. Keyspace: {} | ColumnFamily: {} | Snapshot: {} | BackupName: {}",
                        keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(), snapshotDir.getAbsolutePath(), backupName);

                final Optional<File> snapshotDirectory = getValidSnapshotDirectory(cfDir, snapshotDir, backupName);
                LOGGER.info("Valid snapshot directory: {}", snapshotDirectory.isPresent());
                if (snapshotDirectory.isPresent()) {
                    // Upload this directory
                    LOGGER.info("Going to upload directory: {}", snapshotDirectory.get().getAbsolutePath());
                    uploadDirectory(snapshotDirectory.get().getAbsolutePath(), amazonS3Client, bucketName, key, keyspaceDir.getName(), cfDir.getName());
                } else {
                    LOGGER.warn("Snapshots directory: {} doesn't contain the current backup directory: {}",
                            snapshotDir.getName(), backupName);
                }
            }
        }

        LOGGER.info("Done uploading snapshots for backup: {}", backupName);
    }

    private void uploadDirectory(String localLocation, AmazonS3Client amazonS3Client, String bucketName, String key, String keyspaceName, String cfName) throws IOException {
        LOGGER.info("uploadDirectory() localLocation: {}, AmazonS3Client: {}, bucketName: {}, key: {}, keyspaceName: {}, cfName: {}",
                localLocation, amazonS3Client, bucketName, key, keyspaceName, cfName);
        Files.walkFileTree(FileSystems.getDefault().getPath(localLocation), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                LOGGER.info("Visiting file: {}", file.getAbsolutePath());
                if (file.isFile()) {
                    String fileKey = key + "/" + keyspaceName + "/" + cfName + "/" + file.getName();
                    List<PartETag> partETags = new ArrayList<>();
                    final InitiateMultipartUploadRequest uploadRequest = new InitiateMultipartUploadRequest(bucketName, fileKey);
                    final InitiateMultipartUploadResult uploadResult = amazonS3Client.initiateMultipartUpload(uploadRequest);

                    LOGGER.info("Initiating upload for file: {} | bucket: {} | key: {} | uploadId: {}", file.getAbsolutePath(), bucketName, fileKey, uploadResult.getUploadId());

                    final byte[] buffer = new byte[DEFAULT_PART_SIZE];
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
                            LOGGER.debug("Uploading part: {} | bucket: {} | key: {} | uploadId: {}", partNum, bucketName, fileKey, uploadResult.getUploadId());
                            final UploadPartResult uploadPartResult = amazonS3Client.uploadPart(uploadPartRequest);
                            final PartETag partETag = uploadPartResult.getPartETag();
                            if (!partETag.getETag().equals(new String(Hex.encodeHex(digest)))) {
                                LOGGER.error("Error matching hex");
                                // TODO: Add retry logic
                            }
                            partETags.add(partETag);
                        }
                        LOGGER.debug("Done uploading, now completing bucket: {} | key: {} | uploadId: {}", bucketName, fileKey, uploadResult.getUploadId());
                        final CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName, fileKey, uploadResult.getUploadId(), partETags);
                        amazonS3Client.completeMultipartUpload(completeMultipartUploadRequest);
                        LOGGER.debug("Successfully uploaded the file to S3. Deleting the file now: {}", file.getAbsolutePath());
                        final boolean delete = file.delete();
                        LOGGER.debug("Deletion status: {} for file {}", delete, file.getAbsolutePath());
                    } catch (Exception e) {
                        LOGGER.error("Error uploading file: {}", e);
                        amazonS3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, fileKey, uploadResult.getUploadId()));
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
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Filters unwanted keyspaces and column families
     */
    public boolean isValidBackupDir(File ksDir, File cfDir, File bkDir) {
        if (!bkDir.isDirectory() && !bkDir.exists())
            return false;

        String ksName = ksDir.getName();
        if (SKIP_KEYSPACES.contains(ksName))
            return false;

        String cfName = cfDir.getName();
        if (SKIP_COLUMN_FAMILIES.containsKey(ksName)
                && SKIP_COLUMN_FAMILIES.get(ksName).contains(cfName))
            return false;

        return true;
    }

    private Optional<File> getValidSnapshotDirectory(File cfDir, File snapshotsDir, String snapshotName) {
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

    @Override
    public void download(RestoreContext ctx) throws IOException {
        // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>

        final String accessKey = ctx.getS3AccessKey();
        final String secretKey = ctx.getS3SecretKey();
        // Location of data directory
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();

        final AmazonS3URI backupLocationURI = new AmazonS3URI(ctx.getExternalLocation());
        final String bucketName = backupLocationURI.getBucket();

        String prefixKey = backupLocationURI.getKey() != null ? backupLocationURI.getKey() : "";
        prefixKey = (prefixKey.length() > 0 && !prefixKey.endsWith("/")) ? prefixKey + "/" : prefixKey;
        prefixKey += backupName;
        final String key = prefixKey;

        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3Client amazonS3Client = new AmazonS3Client(basicAWSCredentials);

        final List<String> snapshotFileKeys = listSnapshotFiles(amazonS3Client, bucketName, backupName);

        for (String fileKey : snapshotFileKeys) {

        }
    }

    public List<String> listSnapshotFiles(AmazonS3Client amazonS3Client, String bucketName, String backupName) {
        List<String> snapshotFiles = new ArrayList<>();
        ObjectListing objectListing;
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(backupName);
        do {
            objectListing = amazonS3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                snapshotFiles.add(objectSummary.getKey());
            }
        } while (objectListing.isTruncated());

        return snapshotFiles;
    }
}
