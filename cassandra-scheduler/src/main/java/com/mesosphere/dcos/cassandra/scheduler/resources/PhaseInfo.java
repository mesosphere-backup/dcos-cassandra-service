package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.scheduler.plan.Phase;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PhaseInfo {

    @JsonProperty("id")
    private final int id;
    @JsonProperty("name")
    private final String name;
    @JsonProperty("blocks")
    private final List<BlockInfo> blocks;

    @JsonCreator
    public static PhaseInfo create(
            @JsonProperty("id") final int id,
            @JsonProperty("name") final String name,
            @JsonProperty("blocks") final List<BlockInfo> blocks) {
        return new PhaseInfo(id, name, blocks);
    }

    public static PhaseInfo forPhase(final Phase phase) {
        return create(phase.getId(),
                phase.getName(),
                phase.getBlocks().stream()
                        .map(BlockInfo::forBlock)
                        .collect(Collectors.toList()));
    }

    public PhaseInfo(final int id,
                     final String name,
                     final List<BlockInfo> blocks) {
        this.id = id;
        this.name = name;
        this.blocks = blocks;
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PhaseInfo)) return false;
        PhaseInfo phaseInfo = (PhaseInfo) o;
        return getId() == phaseInfo.getId() &&
                Objects.equals(getName(), phaseInfo.getName()) &&
                Objects.equals(getBlocks(), phaseInfo.getBlocks());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getName(), getBlocks());
    }

    @Override
    public String toString(){
        return JsonUtils.toJsonString(this);
    }
}
