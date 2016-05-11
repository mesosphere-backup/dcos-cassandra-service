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

import java.io.IOException;

/**
 * CassandraStatus is the status object sent from the Executor to the Scheduler
 * to track the status of a Cassandra Daemon.
 */
public class CassandraStatus {

    @JsonProperty("mode")
    final CassandraMode mode;
    @JsonProperty("joined")
    final boolean joined;
    @JsonProperty("rpc_running")
    final boolean rpcRunning;
    @JsonProperty("native_transport_running")
    final boolean nativeTransportRunning;
    @JsonProperty("gossip_initialized")
    final boolean gossipInitialized;
    @JsonProperty("gossip_running")
    final boolean gossipRunning;
    @JsonProperty("host_id")
    final String hostId;
    @JsonProperty("endpoint")
    final String endpoint;
    @JsonProperty("token_count")
    final int tokenCount;
    @JsonProperty("data_center")
    final String dataCenter;
    @JsonProperty("rack")
    final String rack;
    @JsonProperty("version")
    final String version;

    /**
     * Creates a CassandraStatus.
     * @param mode The mode of the Cassandra node.
     * @param joined True if the node has joined the cluster.
     * @param rpcRunning True if the node has rpc running.
     * @param nativeTransportRunning True if the node has CQL transport running.
     * @param gossipInitialized True if gossip with the cluster has been
     *                          initialized.
     * @param gossipRunning True if the node is participating in gossip.
     * @param hostId The id of the node in the ring.
     * @param endpoint The node's endpoint identifier.
     * @param tokenCount The number of tokens assigned to the node.
     * @param dataCenter The datacenter for the node.
     * @param rack The rack for the node.
     * @param version The version of Cassandra the node is running.
     * @return A CassandraStatus constructed from the parameters.
     */
    @JsonCreator
    public static CassandraStatus create(
            @JsonProperty("mode") final CassandraMode mode,
            @JsonProperty("joined") final boolean joined,
            @JsonProperty("rpc_running") final boolean rpcRunning,
            @JsonProperty("native_transport_running")
            final boolean nativeTransportRunning,
            @JsonProperty("gossip_initialized") final boolean gossipInitialized,
            @JsonProperty("gossip_running") final boolean gossipRunning,
            @JsonProperty("host_id") final String hostId,
            @JsonProperty("endpoint") final String endpoint,
            @JsonProperty("token_count") final int tokenCount,
            @JsonProperty("data_center") final String dataCenter,
            @JsonProperty("rack") final String rack,
            @JsonProperty("version") final String version) {

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

    /**
     * Parses a CassandraStatus from a Protocol Buffers representation.
     * @param status The Protocol Buffers representation of a CassandraStatus.
     * @return A CassandraStatus parsed from status.
     */
    public static CassandraStatus parse(
            CassandraProtos.CassandraStatus status) {

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

    /**
     * Parses a CassandraStatus from a byte array containing a Protocol Buffers
     * representation.
     * @param bytes A byte array containing a Protocol Buffers representation
     *              of a CassandraStatus.
     * @return A CassandraStatus parsed from bytes.
     * @throws IOException If a CassandraStatus can not be parsed from bytes.
     */
    public static CassandraStatus parse(byte[] bytes) throws IOException {
        return parse(CassandraProtos.CassandraStatus.parseFrom(bytes));
    }

    /**
     * Constructs a CassandraStatus.
     * @param mode The mode of the Cassandra node.
     * @param joined True if the node has joined the cluster.
     * @param rpcRunning True if the node has rpc running.
     * @param nativeTransportRunning True if the node has CQL transport running.
     * @param gossipInitialized True if gossip with the cluster has been
     *                          initialized.
     * @param gossipRunning True if the node is participating in gossip.
     * @param hostId The id of the node in the ring.
     * @param endpoint The node's endpoint identifier.
     * @param tokenCount The number of tokens assigned to the node.
     * @param dataCenter The datacenter for the node.
     * @param rack The rack for the node.
     * @param version The version of Cassandra the node is running.
     */
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

    /**
     * Gets the mode.
     * @return The mode of the Cassandra node.
     */
    public CassandraMode getMode() {
        return mode;
    }

    /**
     * Gets the Cassandra version.
     * @return The version of Cassandra the node is running.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Test if the node is joined.
     * @return True if the node has joined the ring.
     */
    public boolean isJoined() {
        return joined;
    }

    /**
     * Tests if RPC is running.
     * @return True if the node is running RPS transport.
     */
    public boolean isRpcRunning() {
        return rpcRunning;
    }

    /**
     * Tests if CQL is running
     * @return True if the node is running CQL transport.
     */
    public boolean isNativeTransportRunning() {
        return nativeTransportRunning;
    }

    /**
     * Tests if the gossip is initialized.
     * @return True if the node has initialized Gossip.
     */
    public boolean isGossipInitialized() {
        return gossipInitialized;
    }

    /**
     * Tests if gossip is running.
     * @return True if the node is gossiping within its cluster.
     */
    public boolean isGossipRunning() {
        return gossipRunning;
    }

    /**
     * Gets the node's host id.
     * @return The unique identifier of the node's host in the ring.
     */
    public String getHostId() {
        return hostId;
    }

    /**
     * Gets the node's endpoint.
     * @return The endpoint assigned to the node in the ring.
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Gets the token count.
     * @return The number of tokens assigned to the node.
     */
    public int getTokenCount() {
        return tokenCount;
    }

    /**
     * Gets the node's data center.
     * @return The data center in which the node is located.
     */
    public String getDataCenter() {
        return dataCenter;
    }

    /**
     * Gets the node's rack.
     * @return The rack on which the node is located.
     */
    public String getRack() {
        return rack;
    }

    /**
     * Gets a Protocol Buffers representation.
     * @return A Protocol Buffers representation of the CassandraStatus.
     */
    public CassandraProtos.CassandraStatus toProto() {
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

    /**
     * Gets a byte array containing a Protocol Buffers representation of the
     * the status.
     * @return A byte array containing a Protocol Buffers representation of
     * the nodes status.
     */
    public byte[] toByteArray() {
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
