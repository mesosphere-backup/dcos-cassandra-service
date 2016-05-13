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
package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;

import java.util.UUID;

/**
 * Volume represents the Volume associated with a Cassandra node.
 * @TODO Roll VolumeType into this object.
 */
public class Volume {

    @JsonProperty("path")
    private final String path;
    @JsonProperty("size_mb")
    private final int sizeMb;
    @JsonProperty("id")
    private final String id;

    /**
     * Creates a new Volume.
     * @param path The path of the volume on the host or in the container.
     * @param sizeMb The size of the volume in Mb.
     * @param id The unique identifier of the volume.
     * @return A Volume constructed from the parameters.
     */
    @JsonCreator
    public static Volume create(
            @JsonProperty("path") final String path,
            @JsonProperty("size_mb") final int sizeMb,
            @JsonProperty("id") final String id
    ) {
        return new Volume(path, sizeMb, id);
    }

    /**
     * Constructs a volume from the ProtocolBuffers representation.
     * @param volume The Protocol Buffers representation of the Volume.
     * @return A Volume parsed from volume.
     */
    public static Volume parse(CassandraProtos.Volume volume) {
        return create(volume.getPath(),
                volume.getSizeMb(),
                volume.hasId() ? volume.getId() : "");
    }

    /**
     * Constructs a volume.
     * @param path The path of the volume on the host or in the container.
     * @param sizeMb The size of the volume in Mb.
     * @param id The unique identifier of the volume.
     */
    public Volume(String path, int sizeMb, String id) {
        this.path = path;
        this.sizeMb = sizeMb;
        this.id = id;
    }

    /**
     * Gets the path.
     * @return The path of the volume on the host or in the container.
     */
    public String getPath() {
        return path;
    }

    /**
     * Gets the Volume's size.
     * @return The size of the Volume in Mb.
     */
    public int getSizeMb() {
        return sizeMb;
    }

    /**
     * Gets the Volume's id.
     * @return The universally unique id of the Volume.
     */
    public String getId() {
        return id;
    }

    /**
     * Creates a volume with id.
     * @param id The unique id of the Volume.
     * @return A copy of the Volume with its id set to id.
     */
    public Volume withId(String id) {
        return create(path, sizeMb, id);
    }

    /**
     * Creates a volume with a new unique id.
     * @return A copy of the Volume with its id set a new unqiue id.
     */
    public Volume withId() {
        return withId(UUID.randomUUID().toString());
    }

    /**
     * Gets a Protocol Buffers representation of the Volume.
     * @param role The role associated with the Volume.
     * @param principal The principal associated with teh Volume.
     * @return A Resource constructed from the Volume.
     */
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

    /**
     * Gets a Protocol Buffers representation of the Volume.
     * @return A Protocol Buffers represenation of the Volume.
     */
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
