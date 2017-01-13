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
package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

/**
 * Location represents the rack and data center for a node in a Cassandra
 * cluster.
 */
public class Location {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The default location of the Cassandra location snitch configuration.
     */
    public static final String DEFAULT_FILE = "cassandra-rackdc.properties";

    /**
     * The default location of a Cassandra node (rac1, dc1).
     */
    public static final Location DEFAULT = Location.create("rac1", "dc1");

    @JsonProperty("rack")
    private final String rack;
    @JsonProperty("data_center")
    private final String dataCenter;

    /**
     * Creates a new Location.
     *
     * @param rack       The rack for the Cassandra node.
     * @param dataCenter The data center for the Cassandra node.
     * @return A Location constructed from the parameters.
     */
    @JsonCreator
    public static Location create(
            @JsonProperty("rack") final String rack,
            @JsonProperty("data_center") final String dataCenter) {
        return new Location(rack, dataCenter);
    }

    /**
     * Parses a Location from a Protocol Buffers representation.
     *
     * @param location A Protocol Buffers representation of a Location.
     * @return A Location parsed from the Protocol Buffers representation.
     */
    public static Location parse(CassandraProtos.Location location) {
        return create(location.getRack(), location.getDataCenter());
    }

    /**
     * Parses a Location from a byte array containing a Protocol Buffers
     * representation.
     *
     * @param bytes A byte array containing a Protocol Buffers representation
     *              of a Location.
     * @return A Location parsed from bytes.
     * @throws IOException if a Location could not be parsed from bytes.
     */
    public static Location parse(byte[] bytes)
            throws IOException {
        return Location.parse(CassandraProtos.Location.parseFrom(bytes));
    }

    /**
     * Constructs a Location.
     *
     * @param rack       The rack for the Cassandra node.
     * @param dataCenter The data center for the Cassandra node.
     */
    public Location(final String rack, final String dataCenter) {
        this.rack = rack;
        this.dataCenter = dataCenter;
    }

    /**
     * Gets the data center.
     *
     * @return The data center in which the node is deployed.
     */
    public String getDataCenter() {
        return dataCenter;
    }

    /**
     * Gets the rack.
     *
     * @return The rack on which the node is deployed.
     */
    public String getRack() {
        return rack;
    }

    /**
     * Writes the Location as a Properties file.
     *
     * @param path The Path indicating where the Location will be written.
     * @throws IOException If the Location can not be written to path.
     */
    public void writeProperties(Path path) throws IOException {
        logger.info("Writing properties to path: " + path.toAbsolutePath());
        PrintWriter writer = new PrintWriter(path.toString(), "UTF-8");
        writer.println("dc=" + dataCenter);
        writer.println("rack=" + rack);
        writer.close();
    }

    /**
     * Gets a Protocol Buffers representation of the Location.
     *
     * @return A Protocol Buffers representation of the Location.
     */
    public CassandraProtos.Location toProto() {

        return CassandraProtos.Location.newBuilder()
                .setRack(rack)
                .setDataCenter(dataCenter)
                .build();
    }

    /**
     * Gets a byte array containing a Protocol Buffers representation of the
     * location.
     *
     * @return A byte array containing a Protocol Buffers representation of
     * the location.
     */
    public byte[] toByteArray() {
        return toProto().toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Location)) return false;

        Location location = (Location) o;

        if (rack != null ? !rack.equals(location.rack) : location.rack != null)
            return false;
        return dataCenter != null ? dataCenter.equals(location.dataCenter) :
                location.dataCenter == null;

    }

    @Override
    public int hashCode() {
        int result = rack != null ? rack.hashCode() : 0;
        result = 31 * result + (dataCenter != null ? dataCenter.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
