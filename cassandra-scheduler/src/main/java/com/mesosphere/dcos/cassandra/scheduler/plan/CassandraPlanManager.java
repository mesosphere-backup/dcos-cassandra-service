package com.mesosphere.dcos.cassandra.scheduler.plan;

import org.apache.mesos.Protos;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Plan;
import org.apache.mesos.scheduler.plan.PlanManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;
import java.util.concurrent.atomic.AtomicBoolean;


public class CassandraPlanManager implements PlanManager {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PlanManager.class);

    private volatile Plan plan;
    private final AtomicBoolean interrupted = new AtomicBoolean(false);
    public static final CassandraPlanManager create(){
        return new CassandraPlanManager();
    }
    public CassandraPlanManager() {
        plan = EmptyPlan.get();
    }


    @Override
    public Plan getPlan() {
        return plan;
    }

    public void setPlan(final Plan plan){
        this.plan = plan;
        LOGGER.info("Set plan : current plan = {}",this.plan);
    }

    @Override
    public void update(Observable observable, Protos.TaskStatus status) {

        if (!planIsComplete() &&
                plan.getCurrentPhase() != null &&
                plan.getCurrentPhase().getCurrentBlock() != null) {
            plan.getCurrentPhase().getCurrentBlock().update(status);
        }
    }

    @Override
    public Block getCurrentBlock() {
        if(interrupted.get()){
            LOGGER.info("Plan is interrupted.");
            return null;
        } else if(planIsComplete()) {
            LOGGER.debug("Plan is complete");
            return null;
        } else if(plan.getCurrentPhase() == null){
            LOGGER.info("Current plan phase null");
            return null;
        } else {
            LOGGER.info("Current execution block = {}",
                    plan.getCurrentPhase().getCurrentBlock());
            return plan.getCurrentPhase().getCurrentBlock();
        }
    }

    @Override
    public boolean planIsComplete() {
        return false;
    }

    @Override
    public void proceed() {
        interrupted.set(false);
        LOGGER.info("Set interrupt status : interrupted = {}",
                interrupted.get());
    }

    @Override
    public void interrupt() {
        interrupted.set(true);
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
    public void update(Observable o, Object arg) {

    }
}
