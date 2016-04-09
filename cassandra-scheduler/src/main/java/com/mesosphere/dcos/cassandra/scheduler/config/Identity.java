package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.Protos;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;


public class Identity {

    public static final Serializer<Identity> JSON_SERIALIZER =
            new
                    Serializer<Identity>() {

                        @Override
                        public byte[] serialize(Identity value)
                                throws SerializationException {
                            try {
                                return JsonUtils.MAPPER.writeValueAsBytes(value);
                            } catch (JsonProcessingException ex) {

                                throw new SerializationException(
                                        "Error writing Identity " +
                                                "as JSON formatted byte array", ex);
                            }
                        }

                        @Override
                        public Identity deserialize(byte[] bytes)
                                throws SerializationException {

                            try {
                                return JsonUtils.MAPPER.readValue(bytes,
                                        Identity.class);
                            } catch (IOException ex) {

                                throw new SerializationException("Exception parsing " +
                                        "identity from JSON", ex);
                            }
                        }
                    };


    @JsonCreator
    public static final Identity create(
            @JsonProperty("name") final String name,
            @JsonProperty("id") final String id,
            @JsonProperty("version") final String version,
            @JsonProperty("user") final String user,
            @JsonProperty("cluster") final String cluster,
            @JsonProperty("role") final String role,
            @JsonProperty("principal") final String principal,
            @JsonProperty("failover_timeout_s") final Long failoverTimeoutS,
            @JsonProperty("secret") final String secret,
            @JsonProperty("checkpoint") final boolean checkpoint) {

        return create(
                name,
                id,
                version,
                user,
                cluster,
                role,
                principal,
                Duration.ofSeconds(failoverTimeoutS),
                secret,
                checkpoint);

    }

    public static final Identity create(
            final String name,
            final String id,
            final String version,
            final String user,
            final String cluster,
            final String role,
            final String principal,
            final Duration failoverTimeout,
            final String secret,
            final boolean checkpoint) {
        return new Identity(
                name,
                id,
                version,
                user,
                cluster,
                role,
                principal,
                failoverTimeout,
                secret,
                checkpoint);

    }

    @JsonProperty("name")
    private final String name;
    @JsonProperty("id")
    private final String id;
    @JsonProperty("version")
    private final String version;
    @JsonProperty("cluster")
    private final String cluster;
    @JsonProperty("user")
    private final String user;
    @JsonProperty("role")
    private final String role;
    @JsonProperty("principal")
    private final String principal;
    @JsonIgnore
    private final Duration failoverTimeout;
    @JsonProperty("secret")
    private final String secret;
    @JsonProperty("checkpoint")
    private final boolean checkpoint;

    public Identity(String name,
                    String id,
                    String version,
                    String user,
                    String cluster,
                    String role,
                    String principal,
                    Duration failoverTimeout,
                    String secret, boolean checkpoint) {
        this.name = name;
        this.id = (id != null) ? id : "";
        this.version = version;
        this.user = user;
        this.cluster = cluster;
        this.role = role;
        this.principal = principal;
        this.failoverTimeout = failoverTimeout;
        this.secret = secret;
        this.checkpoint = checkpoint;
    }


    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public String getVersion() {
        return version;
    }

    public String getCluster() {
        return cluster;
    }

    public String getRole() {
        return role;
    }

    public String getUser() {
        return user;
    }

    public String getPrincipal() {
        return principal;
    }

    public Duration getFailoverTimeout() {
        return failoverTimeout;
    }

    @JsonProperty("failover_timeout_s")
    public long getFailoverTimeoutS() {
        return failoverTimeout.getSeconds();
    }

    public boolean isCheckpoint() {
        return checkpoint;
    }

    public String getSecret() {
        return secret;
    }

    public Identity register(final String id) {
        return create(
                name,
                id,
                version,
                user,
                cluster,
                role,
                principal,
                failoverTimeout,
                secret,
                checkpoint);
    }

    public Protos.FrameworkInfo asInfo() {
        Protos.FrameworkInfo.Builder builder = Protos.FrameworkInfo.newBuilder()
                .setName(name)
                .setId(Protos.FrameworkID
                        .newBuilder()
                        .setValue(id))
                .setPrincipal(principal)
                .setRole(role)
                .setUser(user)
                .setCheckpoint(checkpoint)
                .setFailoverTimeout(failoverTimeout.getSeconds());

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Identity)) return false;

        Identity identity = (Identity) o;

        if (isCheckpoint() != identity.isCheckpoint()) return false;
        if (getName() != null ? !getName().equals(
                identity.getName()) : identity.getName() != null) return false;
        if (getId() != null ? !getId().equals(
                identity.getId()) : identity.getId() != null) return false;
        if (getVersion() != null ? !getVersion().equals(
                identity.getVersion()) : identity.getVersion() != null)
            return false;
        if (getCluster() != null ? !getCluster().equals(
                identity.getCluster()) : identity.getCluster() != null)
            return false;
        if (getUser() != null ? !getUser().equals(
                identity.getUser()) : identity.getUser() != null) return false;
        if (getRole() != null ? !getRole().equals(
                identity.getRole()) : identity.getRole() != null) return false;
        if (getPrincipal() != null ? !getPrincipal().equals(
                identity.getPrincipal()) : identity.getPrincipal() != null)
            return false;
        if (getFailoverTimeout() != null ? !getFailoverTimeout().equals(
                identity.getFailoverTimeout()) : identity.getFailoverTimeout() != null)
            return false;
        return getSecret() != null ? getSecret().equals(
                identity.getSecret()) : identity.getSecret() == null;

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
        result = 31 * result + (getCluster() != null ? getCluster().hashCode() : 0);
        result = 31 * result + (getUser() != null ? getUser().hashCode() : 0);
        result = 31 * result + (getRole() != null ? getRole().hashCode() : 0);
        result = 31 * result + (getPrincipal() != null ? getPrincipal().hashCode() : 0);
        result = 31 * result + (getFailoverTimeout() != null ? getFailoverTimeout().hashCode() : 0);
        result = 31 * result + (getSecret() != null ? getSecret().hashCode() : 0);
        result = 31 * result + (isCheckpoint() ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    public Optional<ByteString> readSecretBytes() throws IOException {
        if (secret == null || secret.isEmpty()) {
            return Optional.empty();
        }
        FileInputStream fin = new FileInputStream(new File(secret));
        return Optional.of(ByteString.copyFrom(IOUtils.toByteArray(fin)));
    }
}
