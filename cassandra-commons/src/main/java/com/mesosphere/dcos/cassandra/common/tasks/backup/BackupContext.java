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
package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.util.Objects;

/**
 * BackupContext implements ClusterTaskContext to provide a context for
 * cluster wide backup operations.
 */
public class BackupContext implements ClusterTaskContext, BackupRestoreContext {

    /**
     * Serializer serializes and deserializes a BackupContext to and from a
     * JSON Object.
     */
    public static final Serializer<BackupContext> JSON_SERIALIZER =
        new Serializer<BackupContext>() {
            @Override
            public byte[] serialize(BackupContext value)
                throws SerializationException {
                try {
                    return JsonUtils.MAPPER.writeValueAsBytes(value);
                } catch (JsonProcessingException ex) {
                    throw new SerializationException(
                        "Error writing BackupContext to JSON",
                        ex);
                }
            }

            @Override
            public BackupContext deserialize(byte[] bytes)
                throws SerializationException {
                try {
                    return JsonUtils.MAPPER.readValue(bytes,
                        BackupContext.class);
                } catch (IOException ex) {
                    throw new SerializationException("Error reading " +
                        "BackupContext form JSON", ex);
                }
            }
        };

    @JsonCreator
    public static BackupContext create(
        @JsonProperty("node_id")
        final String nodeId,
        @JsonProperty("name")
        final String name,
        @JsonProperty("external_location")
        final String externalLocation,
        @JsonProperty("local_location")
        final String localLocation,
        @JsonProperty("account_id")
        final String accountId,
        @JsonProperty("secret_key")
        final String secretKey) {
        return new BackupContext(
            nodeId,
            name,
            externalLocation,
            localLocation,
            accountId,
            secretKey);
    }

    @JsonProperty("node_id")
    private final String nodeId;
    @JsonProperty("name")
    private final String name;
    @JsonProperty("external_location")
    private final String externalLocation;
    @JsonProperty("local_location")
    private final String localLocation;
    @JsonProperty("account_id")
    private final String accountId;
    @JsonProperty("secret_key")
    private final String secretKey;


    public BackupContext(final String nodeId,
                         final String name,
                         final String externalLocation,
                         final String localLocation,
                         final String accountId,
                         final String secretKey) {
        this.nodeId = nodeId;
        this.name = name;
        this.externalLocation = externalLocation;
        this.localLocation = localLocation;
        this.accountId = accountId;
        this.secretKey = secretKey;
    }


  /**
   * Gets the name of the backup.
   *
   * @return The name of the backup.
   */
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  /**
   * Gets the external location of the backup.
   *
   * @return The location where the backup files are stored.
   */
  public String getExternalLocation() {
    return externalLocation;
  }

  /**
   * Gets the local location of the backup.
   *
   * @return The local location of the keyspace files that will be backed up.
   */
  @JsonProperty("local_location")
  public String getLocalLocation() {
    return localLocation;
  }

  /**
   * Gets the access key.
   *
   * @return The S3 access key for the bucket or azure storage account where the keyspace files will
   * be stored.
   */
  @JsonProperty("account_id")
  public String getAccountId() {
    return accountId;
  }

  /**
   * Gets the secret key.
   *
   * @return The S3 secret key for the bucket or azure account key where the keyspace files will
   * be stored.
   */
  @JsonProperty("secret_key")
  public String getSecretKey() {
    return secretKey;
  }

    /**
     * Gets the id of the node for the backup.
     *
     * @return The id of the node for the backup.
     */
    @JsonProperty("node_id")
    public String getNodeId() {
        return nodeId;
    }

    @JsonIgnore
    public BackupContext forNode(final String nodeId){
        return create(
            nodeId,
            name,
            externalLocation,
            localLocation,
            accountId,
            secretKey);
    }

    @JsonIgnore
    public BackupContext withLocalLocation(final String localLocation){
        return create(
            nodeId,
            name,
            externalLocation,
            localLocation,
            accountId,
            secretKey);
    }

  @Override
  public String toString() {
    return JsonUtils.toJsonString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BackupContext)) return false;
    BackupContext that = (BackupContext) o;
    return Objects.equals(getNodeId(), that.getNodeId()) &&
      Objects.equals(getName(), that.getName()) &&
      Objects.equals(getExternalLocation(),
        that.getExternalLocation()) &&
      Objects.equals(getLocalLocation(),
        that.getLocalLocation()) &&
      Objects.equals(getAccountId(), that.getAccountId()) &&
      Objects.equals(getSecretKey(), that.getSecretKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getNodeId(), getName(), getExternalLocation(),
      getLocalLocation(), getAccountId(), getSecretKey());
  }
}
