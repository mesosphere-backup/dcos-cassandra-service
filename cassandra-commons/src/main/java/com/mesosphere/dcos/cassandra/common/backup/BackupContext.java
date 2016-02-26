package com.mesosphere.dcos.cassandra.common.backup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;

public class BackupContext implements ClusterTaskContext {
    private String name;

    private String externalLocation;

    private String localLocation;

    private String s3AccessKey;

    private String s3SecretKey;

    public String getName() {
        return name;
    }

    public String getExternalLocation() {
        return externalLocation;
    }

    public String getLocalLocation() {
        return localLocation;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setExternalLocation(String externalLocation) {
        this.externalLocation = externalLocation;
    }

    public void setLocalLocation(String localLocation) {
        this.localLocation = localLocation;
    }

    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }

    @Override
    public String toString() {
        return "BackupContext{" +
                "name='" + name + '\'' +
                ", externalLocation='" + externalLocation + '\'' +
                ", localLocation='" + localLocation + '\'' +
                ", s3AccessKey='" + s3AccessKey + '\'' +
                ", s3SecretKey='" + s3SecretKey + '\'' +
                '}';
    }

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
}
