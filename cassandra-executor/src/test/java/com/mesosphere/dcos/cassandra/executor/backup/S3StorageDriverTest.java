package com.mesosphere.dcos.cassandra.executor.backup;

import com.amazonaws.services.s3.internal.Constants;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
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
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                "name",
                "s3://" + bucketName,
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(bucketName, s3StorageDriver.getBucketName(backupRestoreContext));
    }

    @Test
    public void testGetBucketNameHTTPSProtocol() throws URISyntaxException {
        String bucketName = "cassandraBackup";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                "name",
                "https://s3-us-west-2.amazonaws.com/" + bucketName,
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(bucketName, s3StorageDriver.getBucketName(backupRestoreContext));
    }

    @Test
    public void testGetBucketNameHTTPProtocolIPPort() throws URISyntaxException {
        String bucketName = "cassandraBackup";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                "name",
                "https://127.0.0.1:9092/" + bucketName,
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(bucketName, s3StorageDriver.getBucketName(backupRestoreContext));
    }

    @Test
    public void testGetEmptyPrefixKeyS3Protocol() throws URISyntaxException {
        String backupName = "backup-name";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                backupName,
                "s3://cassandrabackup",
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(backupName, s3StorageDriver.getPrefixKey(backupRestoreContext));
    }

    @Test
    public void testGetNestedPrefixKeyS3Protocol() throws URISyntaxException {
        String backupName = "backup-name";
        String nestedPath = "nested-path";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                backupName,
                "s3://cassandrabackup/" + nestedPath,
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(nestedPath + "/" + backupName, s3StorageDriver.getPrefixKey(backupRestoreContext));
    }

    @Test
    public void testGetEmptyPrefixKeyHTTPSProtocol() throws URISyntaxException {
        String backupName = "backup-name";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                backupName,
                "https://s3-us-west-2.amazonaws.com/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(backupName, s3StorageDriver.getPrefixKey(backupRestoreContext));
    }

    @Test
    public void testGetNestedPrefixKeyHTTPSProtocol() throws URISyntaxException {
        String backupName = "backup-name";
        String nestedPath = "nested-path/hi/yah";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                backupName,
                "https://s3-us-west-2.amazonaws.com/cassandrabackup/" + nestedPath,
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(nestedPath + "/" + backupName, s3StorageDriver.getPrefixKey(backupRestoreContext));
    }

    @Test
    public void testGetEndpointS3Protocol() throws URISyntaxException {
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                "name",
                "s3://cassandrabackup",
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(Constants.S3_HOSTNAME, s3StorageDriver.getEndpoint(backupRestoreContext));
    }

    @Test
    public void testGetEndpointHTTPSProtocol() throws URISyntaxException {
        String endpoint = "https://s3-us-west-2.amazonaws.com";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                "name",
                endpoint + "/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(endpoint, s3StorageDriver.getEndpoint(backupRestoreContext));
    }

    @Test
    public void testGetEndpointHTTPProtocolHostPort() throws URISyntaxException {
        String endpoint = "http://host:1000";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                "name",
                endpoint + "/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(endpoint, s3StorageDriver.getEndpoint(backupRestoreContext));
    }

    @Test
    public void testGetEndpointHTTPProtocolHost() throws URISyntaxException {
        String endpoint = "http://host";
        BackupRestoreContext backupRestoreContext = BackupRestoreContext.create(
                "node-id",
                "name",
                endpoint + "/cassandrabackup",
                "local-location",
                "account-id",
                "secret-key",
                false,
                "existing",
                "username",
                "password");
        Assert.assertEquals(endpoint, s3StorageDriver.getEndpoint(backupRestoreContext));
    }
}
