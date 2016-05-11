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


/**
 * Cleanup context implements ClusterTaskContext to provide a context for
 * cluster wide cleanup operations. Cleanup removes all keys for a node that
 * no longer fall in the token range for the node. Cleanup should be run as a
 * maintenance activity after node addition, node removal, or node
 * replacement.
 * If the key spaces for the context are empty, all non-system key spaces are
 * used.
 * If the column families for the context are empty, all non-system column
 * families are used.
 */
public class CleanupContext implements ClusterTaskContext {

    /**
     * Serializer serializes and deserializes a CleanupContext to and from a
     * JSON object.
     */
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

    /**
     * Creates a new CleanupContext
     * @param nodes The nodes that will be cleaned up.
     * @param keySpaces The key spaces that will be cleaned up. If empty, all
     *                  non-system key spaces will be cleaned up.
     * @param columnFamilies The column families that will be cleaned up. If
     *                       empty, all column families will be clean up.
     * @return A CleanupContext constructed from the parameters.
     */
    @JsonCreator
    public static CleanupContext create(
            @JsonProperty("nodes") final List<String> nodes,
            @JsonProperty("keySpaces") final List<String> keySpaces,
            @JsonProperty("columnFamilies") final List<String> columnFamilies) {
        return new CleanupContext(nodes, keySpaces, columnFamilies);
    }

    @JsonProperty("nodes")
    private final List<String> nodes;
    @JsonProperty("keySpaces")
    private final List<String> keySpaces;
    @JsonProperty("columnFamilies")
    private final List<String> columnFamilies;

    /**
     * Constructs a new CleanupContext
     * @param nodes The nodes that will be cleaned up.
     * @param keySpaces The key spaces that will be cleaned up. If empty, all
     *                  non-system key spaces will be cleaned up.
     * @param columnFamilies The column families that will be cleaned up. If
     *                       empty, all column families will be clean up.
     */
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

    /**
     * Gets the nodes for the cleanup.
     * @return The names of the nodes that will be cleaned.
     */
    public List<String> getNodes() {
        return nodes;
    }

    /**
     * Gets the column families.
     * @return The column families that will be cleaned. If empty, all column
     * families will be cleaned.
     */
    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * Get the key spaces that will be cleaned.
     * @return The key spaces that will be cleaned. If empty, all non-system
     * key spaces will be cleaned.
     */
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
