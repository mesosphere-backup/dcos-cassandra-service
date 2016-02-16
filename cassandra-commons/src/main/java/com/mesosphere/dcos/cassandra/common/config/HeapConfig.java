package com.mesosphere.dcos.cassandra.common.config;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HeapConfig {

    public static final HeapConfig DEFAULT = HeapConfig.create(2048,100);

    @JsonProperty("sizeMb")
    private final int sizeMb;
    @JsonProperty("newMb")
    private final int newMb;
    @JsonCreator
    public static HeapConfig create(
            @JsonProperty("sizeMb") final int sizeMb,
            @JsonProperty("newMb") final int newMb) {
        return new HeapConfig(sizeMb, newMb);
    }

    public static HeapConfig parse( CassandraProtos.HeapConfig heap){
        return create(heap.getSizeMb(), heap.getNewMb());
    }

    public static HeapConfig parse(byte[] bytes) throws IOException {

        return parse(CassandraProtos.HeapConfig.parseFrom(bytes));
    }

    public HeapConfig(final int sizeMb, final int newMb) {
        this.newMb = newMb;
        this.sizeMb = sizeMb;
    }

    public int getSizeMb() {

        return sizeMb;
    }

    public int getNewMb() {
        return newMb;
    }

    public CassandraProtos.HeapConfig toProto() {
        return CassandraProtos.HeapConfig.newBuilder()
                .setSizeMb(sizeMb)
                .setNewMb(newMb)
                .build();
    }

    public byte[] toByteArray() {

        return toProto().toByteArray();
    }


    public Map<String, String> toEnv() {

        return new HashMap<String, String>() {{
            put("MAX_HEAP_SIZE", Integer.toString(sizeMb) +"M");
            put("HEAP_NEWSIZE", Integer.toString(newMb) +"M");
        }};
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HeapConfig that = (HeapConfig) o;

        if (sizeMb != that.sizeMb) return false;
        return newMb == that.newMb;

    }

    @Override
    public int hashCode() {
        int result = sizeMb;
        result = 31 * result + newMb;
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
