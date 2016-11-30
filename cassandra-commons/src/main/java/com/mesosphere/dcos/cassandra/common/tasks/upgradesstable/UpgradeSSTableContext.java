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
package com.mesosphere.dcos.cassandra.common.tasks.upgradesstable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
 * UpgradeSSTable context implements ClusterTaskContext to provide a context for
 * cluster wide upgrade SSTable operations.
 * UpgradeSSTable rewrites older SSTables to the current version of Cassandra.
 * If the key spaces for the context are empty, all non-system key spaces are
 * used.
 * If the column families for the context are empty, all non-system column
 * families are used.
 */
public class UpgradeSSTableContext implements ClusterTaskContext {

    /**
     * Serializer serializes and deserializes a UpgradeSSTableContext to and from a
     * JSON object.
     */
    public static final Serializer<UpgradeSSTableContext> JSON_SERIALIZER =
            new Serializer<UpgradeSSTableContext>() {
                @Override
                public byte[] serialize(UpgradeSSTableContext value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (IOException ex) {
                        throw new SerializationException("Serialization " +
                                "failed", ex);
                    }
                }

                @Override
                public UpgradeSSTableContext deserialize(byte[] bytes)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.readValue(bytes, UpgradeSSTableContext
                                .class);
                    } catch (IOException ex) {
                        throw new SerializationException("Deserialization " +
                                "failed", ex);
                    }
                }
            };

    /**
     * Creates a new UpgradeSSTableContext
     * @param nodes The nodes that will be have their SSTables upgraded.
     * @param keySpaces The key spaces whose SSTables will be upgraded. If empty, all
     *                  non-system key spaces will be cleaned up.
     * @param columnFamilies The column families whose SSTables will be upgraded. If
     *                       empty, all column families will be clean up.
     * @return A UpgradeSSTableContext constructed from the parameters.
     */
    @JsonCreator
    public static UpgradeSSTableContext create(
            @JsonProperty("nodes") final List<String> nodes,
            @JsonProperty("key_spaces") final List<String> keySpaces,
            @JsonProperty("column_families") final List<String> columnFamilies) {
        return new UpgradeSSTableContext(nodes, keySpaces, columnFamilies);
    }

    @JsonProperty("nodes")
    private final List<String> nodes;
    @JsonProperty("key_spaces")
    private final List<String> keySpaces;
    @JsonProperty("column_families")
    private final List<String> columnFamilies;

    /**
     * Constructs a new UpgradeSSTableContext
     * @param nodes The nodes that will be have their SSTables upgraded.
     * @param keySpaces The key spaces whose SSTables will be upgraded. If empty, all
     *                  non-system key spaces will be cleaned up.
     * @param columnFamilies The column families whose SSTables will be upgraded. If
     *                       empty, all column families will be clean up.
     */
    public UpgradeSSTableContext(final List<String> nodes,
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
     * Gets the nodes for the SSTable upgrade.
     * @return The names of the nodes that will be upgraded.
     */
    @JsonIgnore
    public List<String> getNodes() {
        return nodes;
    }

    /**
     * Gets the column families.
     * @return The column families that will be upgraded. If empty, all column
     * families will be upgradeed.
     */
    @JsonIgnore
    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * Get the key spaces that will be upgraded.
     * @return The key spaces that will be upgraded. If empty, all non-system
     * key spaces will be upgraded.
     */
    @JsonIgnore
    public List<String> getKeySpaces() {
        return keySpaces;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UpgradeSSTableContext)) return false;
        UpgradeSSTableContext that = (UpgradeSSTableContext) o;
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
