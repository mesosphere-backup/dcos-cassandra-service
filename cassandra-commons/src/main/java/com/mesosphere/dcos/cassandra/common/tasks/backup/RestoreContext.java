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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.util.Objects;

/**
 * BackupContext implements ClusterTaskContext to provide a context for
 * cluster wide restore operations.
 */
public class RestoreContext implements ClusterTaskContext {

    private String nodeId;
    private String name;
    private String externalLocation;
    private String localLocation;
    private String acccountId;  // s3AccessKey or AccountName (prinicipal for service)
    private String secretKey;  // s3SecretKey or azure key (secret)

    /**
     * Gets the name of the backup.
     *
     * @return The name of the backup.
     */
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
     * @return The local location where the backup files will be downloaded to.
     */
    public String getLocalLocation() {
        return localLocation;
    }

    /**
     * Gets the access key.
     *
     * @return The S3 access key for the bucket or Azure account where the keyspace files are
     * be stored.
     */
    public String getAcccountId() {
        return acccountId;
    }

    /**
     * Gets the secret key.
     *
     * @return The S3 secret key for the bucket or azure key where the keyspace files are
     * be stored.
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets the backup name.
     *
     * @param name The name of the backup.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the external location.
     *
     * @param externalLocation The location where the backup files are
     *                         stored.
     */
    public void setExternalLocation(String externalLocation) {
        this.externalLocation = externalLocation;
    }

    /**
     * Sets the local location.
     *
     * @param localLocation The location where the keyspace files are
     *                      read from.
     */
    public void setLocalLocation(String localLocation) {
        this.localLocation = localLocation;
    }

    /**
     * Sets the S3 access key or azure account.
     *
     * @param acccountId The access key for the bucket where the backup
     *                    files will be stored.
     */
    public void setAcccountId(String acccountId) {
        this.acccountId = acccountId;
    }

    /**
     * Sets the S3 or azure secret key.
     *
     * @param secretKey The secret key for the bucket where teh backup
     *                    files will be stored.
     */
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    /**
     * Gets the id of the node for the backup.
     *
     * @return The id of the node for the backup.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Sets the id of the node for the backup.
     *
     * @param nodeId The id of the node for the backup.
     */
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RestoreContext)) return false;
        RestoreContext that = (RestoreContext) o;
        return Objects.equals(getNodeId(), that.getNodeId()) &&
                Objects.equals(getName(), that.getName()) &&
                Objects.equals(getExternalLocation(),
                        that.getExternalLocation()) &&
                Objects.equals(getLocalLocation(),
                        that.getLocalLocation()) &&
                Objects.equals(getAcccountId(), that.getAcccountId()) &&
                Objects.equals(getSecretKey(), that.getSecretKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getName(), getExternalLocation(),
                getLocalLocation(), getAcccountId(), getSecretKey());
    }

    /**
     * Serializer that serializes a RestoreContext to and from a JSON object.
     */
    public static final Serializer<RestoreContext> JSON_SERIALIZER =
            new Serializer<RestoreContext>() {
                @Override
                public byte[] serialize(RestoreContext value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException(
                                "Error writing RestoreContext to JSON",
                                ex);
                    }
                }

                @Override
                public RestoreContext deserialize(byte[] bytes)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.readValue(bytes,
                                RestoreContext.class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "RestoreContext form JSON", ex);
                    }
                }
            };
}
