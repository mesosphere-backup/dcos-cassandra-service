package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.scheduler.plan.Plan;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by kowens on 2/22/16.
 */
public class PlanInfo {

    @JsonCreator
    public static PlanInfo create(
            @JsonProperty("phases") final List<PhaseInfo> phases){
        return new PlanInfo(phases);
    }

    public static PlanInfo forPlan(final Plan plan){
        return create(plan.getPhases().stream()
                .map(PhaseInfo::forPhase)
                .collect(Collectors.toList()));

    }

        @JsonProperty("phases")
    private final List<PhaseInfo> phases;

    public PlanInfo(final List<PhaseInfo> phases){
        this.phases = phases;
    }

    public List<PhaseInfo> getPhases() {
        return phases;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PlanInfo)) return false;
        PlanInfo planInfo = (PlanInfo) o;
        return Objects.equals(getPhases(), planInfo.getPhases());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPhases());
    }

    @Override
    public String toString(){
        return JsonUtils.toJsonString(this);
    }
}
