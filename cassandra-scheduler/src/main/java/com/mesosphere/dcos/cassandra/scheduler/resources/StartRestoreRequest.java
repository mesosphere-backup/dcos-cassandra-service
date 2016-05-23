package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class StartRestoreRequest {
    @JsonProperty("backup_name")
    @NotEmpty
    private String name;

    @JsonProperty("external_location")
    @NotEmpty
    private String externalLocation;

    @JsonProperty("s3_access_key")
    @NotEmpty
    private String s3AccessKey;

    @JsonProperty("s3_secret_key")
    @NotEmpty
    private String s3SecretKey;

    @JsonProperty("azure_account")
    private String azureAccount;

    @JsonProperty("azure_key")
    private String azureKey;

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

    public String getAzureAccount() {
      return azureAccount;
    }

    public void setAzureAccount(String azureAccount) {
      this.azureAccount = azureAccount;
    }

    public String getAzureKey() {
      return azureKey;
    }

    public void setAzureKey(String azureKey) {
      this.azureKey = azureKey;
    }

    public boolean isValid() {
      return name != null && externalLocation != null &&
        (s3AccessKey != null && s3SecretKey != null) ||
        (azureAccount != null && azureKey != null && externalLocation.startsWith("azure:"));
    }
}
