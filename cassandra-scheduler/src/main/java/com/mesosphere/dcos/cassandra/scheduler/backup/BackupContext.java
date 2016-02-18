package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.mesosphere.dcos.cassandra.scheduler.resources.StartBackupRequest;

public class BackupContext {
    private String name;

    private String externalLocation;

    private String s3AccessKey;

    private String s3SecretKey;

    private BackupContext() {

    }

    public static BackupContext from(StartBackupRequest request) {
        final BackupContext backupContext =
                new BackupContext();
        backupContext.name = request.getName();
        backupContext.externalLocation = request.getExternalLocation();
        backupContext.s3AccessKey = request.getS3AccessKey();
        backupContext.s3SecretKey = request.getS3SecretKey();

        return backupContext;
    }

    public String getName() {
        return name;
    }

    public String getExternalLocation() {
        return externalLocation;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }
}
