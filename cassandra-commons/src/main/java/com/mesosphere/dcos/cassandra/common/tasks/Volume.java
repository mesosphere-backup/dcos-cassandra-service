package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;

import java.util.Optional;
import java.util.UUID;

public class Volume {

    @JsonProperty("path")
    private final String path;
    @JsonProperty("size_mb")
    private final int sizeMb;
    @JsonProperty("id")
    private final String id;

    @JsonCreator
    public static Volume create(
            @JsonProperty("path") final String path,
            @JsonProperty("size_mb") final int sizeMb,
            @JsonProperty("id") final String id
    ) {
        return new Volume(path, sizeMb, id);
    }

    public static Volume parse(CassandraProtos.Volume volume) {
        return create(volume.getPath(),
                volume.getSizeMb(),
                volume.hasId() ? volume.getId() : "");
    }


    public Volume(String path, int sizeMb, String id) {
        this.path = path;
        this.sizeMb = sizeMb;
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public int getSizeMb() {
        return sizeMb;
    }

    public String getId() {
        return id;
    }

    public Volume withId(String id) {
        return create(path, sizeMb, id);
    }

    public Volume withId() {
        return withId(UUID.randomUUID().toString());
    }

    public Resource toResource(String role, String principal) {
        return Resource.newBuilder().setType(Protos.Value.Type.SCALAR)
                .setName("disk")
                .setRole(role)
                .setReservation(Resource.ReservationInfo
                        .newBuilder().setPrincipal(principal))
                .setScalar(Protos.Value.Scalar
                        .newBuilder()
                        .setValue(sizeMb))
                .setDisk(Protos.Resource.DiskInfo.newBuilder()
                        .setPersistence(Protos.Resource.DiskInfo.Persistence
                                .newBuilder().setId(
                                        (id == null || id.isEmpty()) ?
                                                UUID.randomUUID().toString() : id))
                        .setVolume(Protos.Volume.newBuilder()
                                .setContainerPath(path)
                                .setMode(Protos.Volume.Mode.RW
                                )))
                .build();
    }

    public CassandraProtos.Volume toProto() {
        CassandraProtos.Volume.Builder builder =
                CassandraProtos.Volume
                        .newBuilder()
                        .setId(id)
                        .setPath(path)
                        .setSizeMb(sizeMb);
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Volume)) return false;

        Volume volume = (Volume) o;

        if (getSizeMb() != volume.getSizeMb()) return false;
        if (getPath() != null ? !getPath().equals(
                volume.getPath()) : volume.getPath() != null) return false;
        return getId() != null ? getId().equals(
                volume.getId()) : volume.getId() == null;

    }

    @Override
    public int hashCode() {
        int result = getPath() != null ? getPath().hashCode() : 0;
        result = 31 * result + getSizeMb();
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
