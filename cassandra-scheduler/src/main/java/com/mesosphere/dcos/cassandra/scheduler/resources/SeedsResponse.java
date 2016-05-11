package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.util.List;
import java.util.Objects;

public class SeedsResponse {
    @JsonProperty("isSeed")
    private final boolean isSeed;
    @JsonProperty("seeds")
    private final List<String> seeds;

    @JsonCreator
    public static SeedsResponse create(
            @JsonProperty("isSeed") boolean isSeed,
            @JsonProperty("seeds") List<String> seeds) {

        return new SeedsResponse(isSeed,seeds);
    }

    public SeedsResponse(final boolean isSeed,
                         final List<String> seeds){

        this.seeds = ImmutableList.copyOf(seeds);
        this.isSeed = isSeed;
    }

    @JsonIgnore
    public boolean isSeed(){
        return isSeed;
    }

    @JsonIgnore
    public List<String> getSeeds(){
        return seeds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SeedsResponse)) return false;
        SeedsResponse that = (SeedsResponse) o;
        return isSeed() == that.isSeed() &&
                Objects.equals(getSeeds(), that.getSeeds());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSeed(), getSeeds());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
