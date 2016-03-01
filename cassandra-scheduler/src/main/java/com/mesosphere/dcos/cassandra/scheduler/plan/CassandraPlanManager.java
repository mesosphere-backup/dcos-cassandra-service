package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.PlanManager;
import org.apache.mesos.scheduler.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Observable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class CassandraPlanManager implements PlanManager {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PlanManager.class);

    private volatile Plan plan;
    private final AtomicBoolean interrupted = new AtomicBoolean(false);
    private final CassandraPhaseStrategies phaseStrategies;
    private volatile List<PhaseStrategy> strategies;
    @Inject
    public CassandraPlanManager(
            final CassandraPhaseStrategies phaseStrategies) {
        this. plan = EmptyPlan.get();
        this.phaseStrategies = phaseStrategies;
        this.strategies = ImmutableList.copyOf(plan.getPhases().stream().map
                (phase ->
                phaseStrategies.getStrategy(phase)).collect(
                Collectors.toList()));
    }

    private PhaseStrategy getCurrentStrategy(){
        for(PhaseStrategy strategy: strategies){
            if(!strategy.getPhase().isComplete()){
                return strategy;
            }
        }
        return strategies.get(strategies.size() - 1);
    }


    @Override
    public Plan getPlan() {
        return plan;
    }

    @Override
    public Phase getCurrentPhase() {
       return getCurrentStrategy().getPhase();
    }

    public void setPlan(final Plan plan){
        this.plan = plan;
        strategies = ImmutableList.copyOf(this.plan.getPhases().stream().map
                (phase -> this.phaseStrategies.getStrategy(phase)).collect(
                Collectors.toList()));
        LOGGER.info("Set plan : current plan = {}",this.plan);
        LOGGER.info("Phase strategies = {}",strategies);
    }


    @Subscribe
    public void update(Protos.TaskStatus status) {
      getCurrentBlock().update(status);
    }

    @Override
    public Block getCurrentBlock() {
        if(interrupted.get()){
            LOGGER.info("Plan is interrupted.");
            return EmptyPlan.EmptyBlock.get();
        } else if(planIsComplete()) {
            LOGGER.debug("Plan is complete");
            return EmptyPlan.EmptyBlock.get();
        } else {
            Block block = getCurrentStrategy().getCurrentBlock();
            LOGGER.info("Current execution block = {}",block);
            return (block != null) ? block : EmptyPlan.EmptyBlock.get();
        }
    }

    @Override
    public boolean planIsComplete() {
        return getCurrentPhase().isComplete();
    }

    @Override
    public void proceed() {
        interrupted.set(false);
        getCurrentStrategy().proceed();
        LOGGER.info("Set interrupt status : interrupted = {}",
                interrupted.get());
    }

    @Override
    public void interrupt() {
        interrupted.set(true);
        getCurrentStrategy().interrupt();
        LOGGER.info("Set interrupt status : interrupted = {}",
                interrupted.get());
    }

    public boolean isInterrupted(){
        return interrupted.get();
    }

    @Override
    public void restart(int phaseIndex, int blockIndex, boolean force)
            throws IndexOutOfBoundsException {

    }

    @Override
    public Status getStatus() {
        return getCurrentBlock().getStatus();
    }

    @Override
    public Status getPhaseStatus(int phaseId) {
        return getCurrentBlock().getStatus();
    }

    @Override
    public void update(Observable o, Object arg) {

    }
}
