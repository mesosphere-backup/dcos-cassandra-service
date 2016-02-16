package com.mesosphere.dcos.cassandra.common.tasks;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;

public class CassandraStatus {


    @JsonProperty("mode")
    final CassandraMode mode;
    @JsonProperty("joined")
    final boolean joined;
    @JsonProperty("rpcRunning")
    final boolean rpcRunning;
    @JsonProperty("nativeTransportRunning")
    final boolean nativeTransportRunning;
    @JsonProperty("gossipInitialized")
    final boolean gossipInitialized;
    @JsonProperty("gossipRunning")
    final boolean gossipRunning;
    @JsonProperty("hostId")
    final String hostId;
    @JsonProperty("endpoint")
    final String endpoint;
    @JsonProperty("tokenCount")
    final int tokenCount;
    @JsonProperty("dataCenter")
    final String dataCenter;
    @JsonProperty("rack")
    final String rack;
    @JsonProperty("version")
    final String version;

    @JsonCreator
    public static CassandraStatus create(
            @JsonProperty("mode") final CassandraMode mode,
            @JsonProperty("joined") final boolean joined,
            @JsonProperty("rpcRunning")final boolean rpcRunning,
            @JsonProperty("nativeTransportRunning")
            final boolean nativeTransportRunning,
            @JsonProperty("gossipInitialized") final boolean gossipInitialized,
            @JsonProperty("gossipRunning")final boolean gossipRunning,
            @JsonProperty("hostId")final String hostId,
            @JsonProperty("endpoint") final String endpoint,
            @JsonProperty("tokenCount")final int tokenCount,
            @JsonProperty("dataCenter")final String dataCenter,
            @JsonProperty("rack") final String rack,
            @JsonProperty("version")final String version) {

        return new CassandraStatus(
                mode,
                joined,
                rpcRunning,
                nativeTransportRunning,
                gossipInitialized,
                gossipRunning,
                hostId,
                endpoint,
                tokenCount,
                dataCenter,
                rack,
                version);
    }

    public static CassandraStatus parse(
            CassandraProtos.CassandraStatus status){

        return create(
                CassandraMode.values()[status.getMode()],
                status.getJoined(),
                status.getRpcServerRunning(),
                status.getNativeTransportRunning(),
                status.getGossipInitialized(),
                status.getGossipRunning(),
                status.getHostId(),
                status.getEndpoint(),
                status.getTokenCount(),
                status.getDataCenter(),
                status.getRack(),
                status.getVersion());
    }

    public static CassandraStatus parse(byte [] bytes) throws IOException{
        return parse(CassandraProtos.CassandraStatus.parseFrom(bytes));
    }

    public CassandraStatus(
            CassandraMode mode,
            boolean joined,
            boolean rpcRunning,
            boolean nativeTransportRunning,
            boolean gossipInitialized,
            boolean gossipRunning,
            String hostId,
            String endpoint,
            int tokenCount,
            String dataCenter,
            String rack,
            String version) {
        this.mode = mode;
        this.joined = joined;
        this.rpcRunning = rpcRunning;
        this.nativeTransportRunning = nativeTransportRunning;
        this.gossipInitialized = gossipInitialized;
        this.gossipRunning = gossipRunning;
        this.hostId = hostId;
        this.endpoint = endpoint;
        this.tokenCount = tokenCount;
        this.dataCenter = dataCenter;
        this.rack = rack;
        this.version = version;
    }

    public CassandraMode getMode(){
        return mode;
    }

    public String getVersion() {
        return version;
    }

    public boolean isJoined() {
        return joined;
    }

    public boolean isRpcRunning() {
        return rpcRunning;
    }

    public boolean isNativeTransportRunning() {
        return nativeTransportRunning;
    }

    public boolean isGossipInitialized() {
        return gossipInitialized;
    }

    public boolean isGossipRunning() {
        return gossipRunning;
    }

    public String getHostId() {
        return hostId;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public int getTokenCount() {
        return tokenCount;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getRack() {
        return rack;
    }

    public CassandraProtos.CassandraStatus toProto(){
        return CassandraProtos.CassandraStatus.newBuilder()
                .setMode(mode.ordinal())
                .setJoined(joined)
                .setRpcServerRunning(rpcRunning)
                .setNativeTransportRunning(nativeTransportRunning)
                .setGossipInitialized(gossipInitialized)
                .setGossipRunning(gossipRunning)
                .setTokenCount(tokenCount)
                .setHostId(hostId)
                .setEndpoint(endpoint)
                .setDataCenter(dataCenter)
                .setRack(rack)
                .setVersion(version)
                .build();
    }

    public byte [] toByteArray(){
        return toProto().toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraStatus)) return false;

        CassandraStatus that = (CassandraStatus) o;

        if (isJoined() != that.isJoined()) return false;
        if (isRpcRunning() != that.isRpcRunning()) return false;
        if (isNativeTransportRunning() != that.isNativeTransportRunning())
            return false;
        if (isGossipInitialized() != that.isGossipInitialized()) return false;
        if (isGossipRunning() != that.isGossipRunning()) return false;
        if (getTokenCount() != that.getTokenCount()) return false;
        if (getMode() != that.getMode()) return false;
        if (getHostId() != null ? !getHostId().equals(
                that.getHostId()) : that.getHostId() != null) return false;
        if (getEndpoint() != null ? !getEndpoint().equals(
                that.getEndpoint()) : that.getEndpoint() != null) return false;
        if (getDataCenter() != null ? !getDataCenter().equals(
                that.getDataCenter()) : that.getDataCenter() != null)
            return false;
        if (getRack() != null ? !getRack().equals(
                that.getRack()) : that.getRack() != null) return false;
        return getVersion() != null ? getVersion().equals(
                that.getVersion()) : that.getVersion() == null;

    }

    @Override
    public int hashCode() {
        int result = getMode() != null ? getMode().hashCode() : 0;
        result = 31 * result + (isJoined() ? 1 : 0);
        result = 31 * result + (isRpcRunning() ? 1 : 0);
        result = 31 * result + (isNativeTransportRunning() ? 1 : 0);
        result = 31 * result + (isGossipInitialized() ? 1 : 0);
        result = 31 * result + (isGossipRunning() ? 1 : 0);
        result = 31 * result + (getHostId() != null ? getHostId().hashCode() : 0);
        result = 31 * result + (getEndpoint() != null ? getEndpoint().hashCode() : 0);
        result = 31 * result + getTokenCount();
        result = 31 * result + (getDataCenter() != null ? getDataCenter().hashCode() : 0);
        result = 31 * result + (getRack() != null ? getRack().hashCode() : 0);
        result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
