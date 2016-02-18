package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StartBackupRequest {
    @JsonProperty("name")
    private String name;

    @JsonProperty("external-location")
    private String externalLocation;

    @JsonProperty("s3-access-key")
    private String s3AccessKey;

    @JsonProperty("s3-secret-key")
    private String s3SecretKey;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExternalLocation() {
        return externalLocation;
    }

    public void setExternalLocation(String externalLocation) {
        this.externalLocation = externalLocation;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }
}
