package com.mesosphere.dcos.cassandra.executor.backup;

import com.amazonaws.services.s3.internal.Constants;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;

/**
 * This class tests the S3StorageDriver class.
 */
public class S3StorageDriverTest {
    private S3StorageDriver s3StorageDriver;

    @Before
    public void beforeEach() {
        s3StorageDriver = new S3StorageDriver();
    }

    @Test
    public void testGetBucketNameS3Protocol() throws URISyntaxException {
        String bucketName = "cassandraBackup";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                "name",
                "s3://" + bucketName,
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(bucketName, s3StorageDriver.getBucketName(backupContext));
    }

    @Test
    public void testGetBucketNameHTTPSProtocol() throws URISyntaxException {
        String bucketName = "cassandraBackup";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                "name",
                "https://s3-us-west-2.amazonaws.com/" + bucketName,
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(bucketName, s3StorageDriver.getBucketName(backupContext));
    }

    @Test
    public void testGetBucketNameHTTPProtocolIPPort() throws URISyntaxException {
        String bucketName = "cassandraBackup";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                "name",
                "https://127.0.0.1:9092/" + bucketName,
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(bucketName, s3StorageDriver.getBucketName(backupContext));
    }

    @Test
    public void testGetEmptyPrefixKeyS3Protocol() throws URISyntaxException {
        String backupName = "backup-name";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                backupName,
                "s3://cassandrabackup",
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(backupName, s3StorageDriver.getPrefixKey(backupContext));
    }

    @Test
    public void testGetNestedPrefixKeyS3Protocol() throws URISyntaxException {
        String backupName = "backup-name";
        String nestedPath = "nested-path";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                backupName,
                "s3://cassandrabackup/" + nestedPath,
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(nestedPath + "/" + backupName, s3StorageDriver.getPrefixKey(backupContext));
    }

    @Test
    public void testGetEmptyPrefixKeyHTTPSProtocol() throws URISyntaxException {
        String backupName = "backup-name";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                backupName,
                "https://s3-us-west-2.amazonaws.com/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(backupName, s3StorageDriver.getPrefixKey(backupContext));
    }

    @Test
    public void testGetNestedPrefixKeyHTTPSProtocol() throws URISyntaxException {
        String backupName = "backup-name";
        String nestedPath = "nested-path";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                backupName,
                "https://s3-us-west-2.amazonaws.com/cassandrabackup/" + nestedPath,
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(nestedPath + "/" + backupName, s3StorageDriver.getPrefixKey(backupContext));
    }

    @Test
    public void testGetEndpointS3Protocol() throws URISyntaxException {
        BackupContext backupContext = BackupContext.create(
                "node-id",
                "name",
                "s3://cassandrabackup",
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(Constants.S3_HOSTNAME, s3StorageDriver.getEndpoint(backupContext));
    }

    @Test
    public void testGetEndpointHTTPSProtocol() throws URISyntaxException {
        String endpoint = "https://s3-us-west-2.amazonaws.com";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                "name",
                endpoint + "/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(endpoint, s3StorageDriver.getEndpoint(backupContext));
    }

    @Test
    public void testGetEndpointHTTPProtocolHostPort() throws URISyntaxException {
        String endpoint = "http://host:1000";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                "name",
                endpoint + "/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(endpoint, s3StorageDriver.getEndpoint(backupContext));
    }

    @Test
    public void testGetEndpointHTTPProtocolHost() throws URISyntaxException {
        String endpoint = "http://host";
        BackupContext backupContext = BackupContext.create(
                "node-id",
                "name",
                endpoint + "/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key");
        Assert.assertEquals(endpoint, s3StorageDriver.getEndpoint(backupContext));
    }
}
