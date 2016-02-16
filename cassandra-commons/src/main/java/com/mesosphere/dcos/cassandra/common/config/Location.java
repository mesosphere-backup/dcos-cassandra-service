package com.mesosphere.dcos.cassandra.common.config;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class Location {

    public static final String DEFAULT_FILE = "cassandra-rackdc.properties";
    public static final Location DEFAULT = Location.create("rac1","dc1");

    @JsonProperty("rack")
    private final String rack;
    @JsonProperty("dataCenter")
    private final String dataCenter;

    @JsonCreator
    public static Location create(
            @JsonProperty("rack") final String rack,
            @JsonProperty("dataCenter") final String dataCenter) {
        return new Location(rack, dataCenter);
    }

    public static Location parse(CassandraProtos.Location location) {
        return create(location.getRack(), location.getDataCenter());
    }

    public static Location parse(byte[] bytes)
            throws IOException {
        return Location.parse(CassandraProtos.Location.parseFrom(bytes));
    }

    public Location(final String rack, final String dataCenter) {
        this.rack = rack;
        this.dataCenter = dataCenter;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getRack() {
        return rack;
    }

    public Properties toProperties() {

        Properties properties = new Properties();
        properties.setProperty("rack", rack);
        properties.setProperty("dc", dataCenter);
        return properties;
    }

    public void writeProperties(Path path) throws IOException {

        FileOutputStream stream = new FileOutputStream(path.toFile());
        try {
            toProperties().store(stream, "DCOS got yo Cassandra!");
        } finally {
            stream.close();
        }
    }

    public CassandraProtos.Location toProto(){

        return CassandraProtos.Location.newBuilder()
                .setRack(rack)
                .setDataCenter(dataCenter)
                .build();
    }

    public byte [] toByteArray(){
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
