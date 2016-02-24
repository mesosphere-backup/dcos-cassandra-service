package com.mesosphere.dcos.cassandra.scheduler.plan;


import org.apache.mesos.scheduler.plan.*;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EmptyPhase implements Phase{
    private static final EmptyPhase instance = new EmptyPhase();

    private PhaseStrategy strategy = new DefaultInstallStrategy(this);

    public static EmptyPhase get(){
        return instance;
    }

    private EmptyPhase(){}
    @Override
    public List<? extends Block> getBlocks() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Block getCurrentBlock() {
        return EmptyBlock.get();
    }

    @Override
    public int getId() {
        return Integer.MIN_VALUE;
    }

    @Override
    public String getName() {
        return "EMPTY";
    }

    @Override
    public Status getStatus() {
        return Status.Complete;
    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public PhaseStrategy getStrategy() {
        return strategy;
    }
}
