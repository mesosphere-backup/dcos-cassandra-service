package com.mesosphere.dcos.cassandra.common.tasks.cleanup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CleanupContext implements ClusterTaskContext {

    public static final Serializer<CleanupContext> JSON_SERIALIZER =
            new Serializer<CleanupContext>() {
                @Override
                public byte[] serialize(CleanupContext value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (IOException ex) {
                        throw new SerializationException("Serialization " +
                                "failed", ex);
                    }
                }

                @Override
                public CleanupContext deserialize(byte[] bytes)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.readValue(bytes, CleanupContext
                                .class);
                    } catch (IOException ex) {
                        throw new SerializationException("Deserialization " +
                                "failed", ex);
                    }
                }
            };

    @JsonCreator
    public static CleanupContext create(
            @JsonProperty("nodes") final List<String> nodes,
            @JsonProperty("keySpaces") final List<String> keySpaces,
            @JsonProperty("columnFamilies") final List<String> columnFamilies) {
        return new CleanupContext( nodes,keySpaces,columnFamilies);
    }

    @JsonProperty("nodes")
    private final List<String> nodes;
    @JsonProperty("keySpaces")
    private final List<String> keySpaces;
    @JsonProperty("columnFamilies")
    private final List<String> columnFamilies;

    public CleanupContext(final List<String> nodes,
                          final List<String> keySpaces,
                          final List<String> columnFamilies) {
        this.nodes = (nodes == null) ? Collections.emptyList() : nodes;
        this.keySpaces = (keySpaces == null) ?
                Collections.emptyList() :
                keySpaces;
        this.columnFamilies = (columnFamilies == null) ?
                Collections.emptyList() :
                columnFamilies;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    public List<String> getKeySpaces() {
        return keySpaces;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CleanupContext)) return false;
        CleanupContext that = (CleanupContext) o;
        return Objects.equals(getNodes(), that.getNodes()) &&
                Objects.equals(getKeySpaces(), that.getKeySpaces()) &&
                Objects.equals(getColumnFamilies(),
                        that.getColumnFamilies());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), getKeySpaces(), getColumnFamilies());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
