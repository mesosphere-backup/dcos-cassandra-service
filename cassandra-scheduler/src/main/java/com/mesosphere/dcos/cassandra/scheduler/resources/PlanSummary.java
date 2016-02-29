package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.scheduler.plan.*;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Created by kowens on 2/22/16.
 */
public class PlanSummary {

    public static PlanSummary create(
            @JsonProperty("phases") final int phases,
            @JsonProperty("blocks") final int blocks,
            @JsonProperty("status") final Status status,
            @JsonProperty("block") final Map<String, Object> currentBlock,
            @JsonProperty("phase") final Map<String, Object> currentPhase){

        return new PlanSummary(
                phases,
                blocks,
                status,
                currentBlock,
                currentPhase);
    }

    public static PlanSummary forPlan(final PlanManager manager){

       return create(manager.getPlan().getPhases().size(),
               manager.getPlan().getPhases().stream().map(phase -> phase.getBlocks()
                       .size
                       ()).reduce( 0,
                       (a, b) -> a + b).intValue(),
                manager.getStatus(),
                getBlockSummary(manager.getCurrentBlock()),
                getPhaseSummary(manager.getCurrentPhase())
        );
    }

    private static Map<String,Object> getBlockSummary(final Block block){
        if(block == null){
            return Collections.emptyMap();
        } else {

            return ImmutableMap.of(
                    "index",block.getId(),
                    "name",block.getName());
        }
    }

    private static Map<String,Object> getPhaseSummary(final Phase phase){
        if(phase == null){
            return Collections.emptyMap();
        } else {

            return ImmutableMap.of(
                    "index",phase.getId(),
                    "name",phase.getName());
        }
    }

    @JsonProperty("phases")
    private final int phases;
    @JsonProperty("blocks")
    private final int blocks;
    @JsonProperty("status")
    private final Status status;
    @JsonProperty("block")
    private final Map<String, Object> currentBlock;
    @JsonProperty("phase")
    private final Map<String, Object> currentPhase;

    public PlanSummary(
            final int phases,
            final int blocks,
            final Status status,
            final Map<String, Object> currentBlock,
            final Map<String, Object> currentPhase
    ) {
        this.phases = phases;
        this.blocks = blocks;
        this.status = status;
        this.currentBlock = currentBlock;
        this.currentPhase = currentPhase;
    }

    public int getBlocks() {
        return blocks;
    }

    public Map<String, Object> getCurrentBlock() {
        return currentBlock;
    }

    public Map<String, Object> getCurrentPhase() {
        return currentPhase;
    }

    public int getPhases() {
        return phases;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PlanSummary)) return false;
        PlanSummary that = (PlanSummary) o;
        return getPhases() == that.getPhases() &&
                getBlocks() == that.getBlocks() &&
                getStatus() == that.getStatus() &&
                Objects.equals(getCurrentBlock(),
                        that.getCurrentBlock()) &&
                Objects.equals(getCurrentPhase(), that.getCurrentPhase());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPhases(), getBlocks(),
                getStatus(),
                getCurrentBlock(), getCurrentPhase());
    }

    @Override
    public String toString(){
        return JsonUtils.toJsonString(this);
    }
}
