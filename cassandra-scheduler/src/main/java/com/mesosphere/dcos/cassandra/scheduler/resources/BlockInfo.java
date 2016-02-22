package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;

import java.util.Objects;

/**
 * Created by kowens on 2/22/16.
 */
public class BlockInfo {

    @JsonProperty("id")
    private final int id;
    @JsonProperty("status")
    private final Status status;
    @JsonProperty("name")
    private final String name;

    @JsonCreator
    public static BlockInfo create(
            @JsonProperty("id") final int id,
            @JsonProperty("status") final Status status,
            @JsonProperty("name") final String name) {
        return new BlockInfo(id, status, name);
    }

    public static BlockInfo forBlock(Block block) {
        return create(block.getId(), block.getStatus(), block.getName());

    }

    public BlockInfo(final int id, final Status status, final String name) {
        this.id = id;
        this.status = status;
        this.name = name;

    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BlockInfo)) return false;
        BlockInfo blockInfo = (BlockInfo) o;
        return getId() == blockInfo.getId() &&
                getStatus() == blockInfo.getStatus() &&
                Objects.equals(getName(), blockInfo.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getStatus(), getName());
    }

    @Override
    public String toString(){
        return JsonUtils.toJsonString(this);
    }
}
