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
package com.mesosphere.dcos.cassandra.common.tasks.repair;

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
 * RepairContext implements ClusterTaskContext to provide a context for
 * cluster wide sequential, primary range, anti-entropy repair.
 */
public class RepairContext implements ClusterTaskContext {

    /**
     * Serializer serializes and deserializes RepairContext to and from a JSON
     * object.
     */
    public static final Serializer<RepairContext> JSON_SERIALIZER =
            new Serializer<RepairContext>() {
                @Override
                public byte[] serialize(RepairContext value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (IOException ex) {
                        throw new SerializationException("Serialization " +
                                "failed", ex);
                    }
                }

                @Override
                public RepairContext deserialize(byte[] bytes)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.readValue(bytes, RepairContext
                                .class);
                    } catch (IOException ex) {
                        throw new SerializationException("Deserialization " +
                                "failed", ex);
                    }
                }
            };

    /**
     * Creates a new RepairContext.
     * @param nodes The nodes on which repair will be performed.
     * @param keySpaces The key spaces that will be repaired. If empty, all
     *                  non-system key spaces will be repaired.
     * @param columnFamilies The column families that will be repaired. If
     *                       empty, all column families for the indicated key
     *                       spaces will be repaired.
     * @return A new RepairContext.
     */
    @JsonCreator
    public static RepairContext create(
            @JsonProperty("nodes") final List<String> nodes,
            @JsonProperty("keySpaces") final List<String> keySpaces,
            @JsonProperty("columnFamilies") final List<String> columnFamilies) {
        return new RepairContext( nodes,keySpaces,columnFamilies);
    }

    @JsonProperty("nodes")
    private final List<String> nodes;
    @JsonProperty("keySpaces")
    private final List<String> keySpaces;
    @JsonProperty("columnFamilies")
    private final List<String> columnFamilies;

    /**
     * Constructs a new RepairContext.
     * @param nodes The nodes on which repair will be performed.
     * @param keySpaces The key spaces that will be repaired. If empty, all
     *                  non-system key spaces will be repaired.
     * @param columnFamilies The column families that will be repaired. If
     *                       empty, all column families for the indicated key
     *                       spaces will be repaired.
     */
    public RepairContext(final List<String> nodes,
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
     * Gets the nodes to repair.
     * @return The nodes that will be repaired.
     */
    public List<String> getNodes() {
        return nodes;
    }

    /**
     * Gets the column families.
     * @return The column families that will be repaired. If empty, all
     * column families for the indicated key spaces will be repaired.
     */
    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * Gets the key spaces.
     * @return The key spaces that will be repaired. If empty, all non-system
     * key spaces will be repaired.
     */
    public List<String> getKeySpaces() {
        return keySpaces;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RepairContext)) return false;
        RepairContext that = (RepairContext) o;
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
