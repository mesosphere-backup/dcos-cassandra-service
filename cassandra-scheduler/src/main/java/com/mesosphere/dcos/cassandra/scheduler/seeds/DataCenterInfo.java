package com.mesosphere.dcos.cassandra.scheduler.seeds;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.util.List;
import java.util.Objects;

public class DataCenterInfo {

    @JsonProperty("data_center")
    private final String datacenter;
    @JsonProperty("url")
    private final String url;
    @JsonProperty("seeds")
    private final List<String> seeds;

    @JsonCreator
    public static DataCenterInfo create(
            @JsonProperty("data_center") String datacenter,
            @JsonProperty("url") String url,
            @JsonProperty("seeds") List<String> seeds) {
        return new DataCenterInfo(datacenter,url,seeds);
    }

    public DataCenterInfo(String datacenter, String url, List<String> seeds) {
        this.datacenter = datacenter;
        this.seeds = ImmutableList.copyOf(seeds);
        this.url = url;
    }

    public List<String> getSeeds() {
        return seeds;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataCenterInfo)) return false;
        DataCenterInfo that = (DataCenterInfo) o;
        return Objects.equals(getDatacenter(), that.getDatacenter()) &&
                Objects.equals(getUrl(), that.getUrl()) &&
                Objects.equals(getSeeds(), that.getSeeds());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDatacenter(), getUrl(), getSeeds());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
