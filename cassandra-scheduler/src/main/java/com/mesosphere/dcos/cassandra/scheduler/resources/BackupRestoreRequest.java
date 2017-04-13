package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskRequest;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotEmpty;

public class BackupRestoreRequest implements ClusterTaskRequest {
  @JsonProperty("backup_name")
  @NotEmpty
  private String name;

  @JsonProperty("external_location")
  @NotEmpty
  private String externalLocation;

  @JsonProperty("s3_access_key")
  private String s3AccessKey;

  @JsonProperty("s3_secret_key")
  private String s3SecretKey;

  @JsonProperty("azure_account")
  private String azureAccount;

  @JsonProperty("azure_key")
  private String azureKey;

  @JsonProperty("uses_emc")
  private Boolean usesEmc;

  @JsonProperty("restore_type")
  private String restoreType;
  
  @JsonProperty("username")
  private String username;
  
  @JsonProperty("password")
  private String password;
  

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

  
  public String getUsername() {
	  if (StringUtils.isBlank(username)) {
		  username= "nouser";
	  }
	  return username;
  }
  public void setUsername(String username) {
	  this.username = username;
  }
  
  public String getPassword() {
	  if (StringUtils.isBlank(password)) {
		  password= "nopassword";
	  }
	  return password;
  }
  
  public void setPassword(String password) {
	  this.password = password;
  }
  
  public String getRestoreType() {


    if (StringUtils.isNotBlank(restoreType)) {
      return restoreType;
    } else {
      return "existing";
    }
  }

  public void setRestoreType(String restoreType) { this.restoreType = restoreType; }

  public boolean usesEmc() {
    if (usesEmc != null) {
      return usesEmc;
    } else {
      return false;
    }
  }

  public boolean isValid() {
    return (StringUtils.isNotBlank(name) && externalLocation != null)
            && (isValidS3Request() || isValidAzureRequest())
            && isValidRestoreType();
  }

  private boolean isValidS3Request() {
    return s3AccessKey != null
            && s3SecretKey != null
            && isValidS3ExternalLocation();
  }

  private boolean isValidS3ExternalLocation() {
    return externalLocation.startsWith("s3:")
            || externalLocation.startsWith("http:")
            || externalLocation.startsWith("https:");
  }

  private boolean isValidAzureRequest() {
    return azureAccount != null && azureKey != null && externalLocation.startsWith("azure:");
  }

  private boolean isValidRestoreType() {
    return restoreType == null || restoreType.isEmpty() ? true: restoreType.matches("existing|new");
  }

  @Override
  public String toString() {
    return "BackupRestoreRequest{" +
            "name='" + name + '\'' +
            ", externalLocation='" + externalLocation + '\'' +
            ", s3AccessKey='" + s3AccessKey + '\'' +
            ", s3SecretKey='" + s3SecretKey + '\'' +
            ", azureAccount='" + azureAccount + '\'' +
            ", azureKey='" + azureKey + '\'' +
            ", usesEmc='" + usesEmc + '\'' +
            ", restoreType='" + restoreType + '\'' +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            '}';
  }


  public BackupRestoreContext toContext() {
    String accountId;
    String secretKey;
    if (isAzure(getExternalLocation())) {
      accountId = getAzureAccount();
      secretKey = getAzureKey();
    } else {
      accountId = getS3AccessKey();
      secretKey = getS3SecretKey();
    }

    return BackupRestoreContext.create(
        "", // node_id
        getName(),
        getExternalLocation(),
        "", // local_location
        accountId,
        secretKey,
        usesEmc(),
        getRestoreType(),
        getUsername(),
        getPassword());
  }

  private static boolean isAzure(String externalLocation) {
    return StringUtils.isNotEmpty(externalLocation) && externalLocation.startsWith("azure:");
  }
}
