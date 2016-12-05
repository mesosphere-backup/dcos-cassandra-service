package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskRequest;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.util.*;
import java.util.stream.Collectors;

public class UpgradeSSTableRequest implements ClusterTaskRequest {

    public static final String ALL = "*";

    @JsonCreator
    public static UpgradeSSTableRequest create(
            @JsonProperty("nodes") final List<String> nodes,
            @JsonProperty("key_spaces") final List<String> keySpaces,
            @JsonProperty("column_families") final List<String>
                    columnFamilies) {
        return new UpgradeSSTableRequest(nodes, keySpaces, columnFamilies);
    }

    @JsonProperty("nodes")
    private final List<String> nodes;
    @JsonProperty("key_spaces")
    private final List<String> keySpaces;
    @JsonProperty("column_families")
    private final List<String> columnFamiles;

    public UpgradeSSTableRequest(
            final List<String> nodes,
            final List<String> keySpaces,
            final List<String> columnFamiles) {

        this.nodes = (nodes == null) ? Collections.emptyList() : nodes;
        this.keySpaces = (keySpaces == null) ? Collections.emptyList() :
                keySpaces;
        this.columnFamiles = (columnFamiles == null) ? Collections.emptyList() :
                columnFamiles;
    }


    public List<String> getColumnFamiles() {
        return columnFamiles;
    }

    public List<String> getKeySpaces() {
        return keySpaces;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public boolean isValid(){
        return !this.nodes.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UpgradeSSTableRequest)) return false;
        UpgradeSSTableRequest that = (UpgradeSSTableRequest) o;
        return Objects.equals(getNodes(), that.getNodes()) &&
                Objects.equals(getKeySpaces(), that.getKeySpaces()) &&
                Objects.equals(getColumnFamiles(),
                        that.getColumnFamiles());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), getKeySpaces(), getColumnFamiles());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    public UpgradeSSTableContext toContext(CassandraState cassandraState) {
        return UpgradeSSTableContext.create(
                new ArrayList<>(getNodes(cassandraState)),
                getKeySpaces(),
                getColumnFamiles());
    }

    private Set<String> getNodes(CassandraState cassandraState) {
        final Set<String> allDaemons = cassandraState.getDaemons().keySet();
        if (getNodes().size() == 1 &&
                getNodes().get(0).equals(UpgradeSSTableRequest.ALL)) {
            return allDaemons;
        } else {
            return getNodes().stream()
                    .filter(node -> allDaemons.contains(node))
                    .collect(Collectors.toSet());
        }
    }
}
