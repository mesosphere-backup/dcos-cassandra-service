package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.backup.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.scheduler.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractClusterTaskPhase<B extends Block, C extends ClusterTaskContext> implements Phase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractClusterTaskPhase.class);

    protected int servers;
    protected final EventBus eventBus;
    protected C context;
    protected List<B> blocks;
    protected final CassandraTasks cassandraTasks;
    protected ClusterTaskOfferRequirementProvider provider;
    protected PhaseStrategy strategy = new DefaultInstallStrategy(this);

    public AbstractClusterTaskPhase(
            C context,
            int servers,
            CassandraTasks cassandraTasks,
            EventBus eventBus,
            ClusterTaskOfferRequirementProvider provider) {
        this.servers = servers;
        this.eventBus = eventBus;
        this.provider = provider;
        this.context = context;
        this.cassandraTasks = cassandraTasks;
        this.blocks = createBlocks();
    }

    protected abstract List<B> createBlocks();

    @Override
    public List<? extends Block> getBlocks() {
        return blocks;
    }

    @Override
    public Block getCurrentBlock() {
        Block currentBlock = null;
        if (!CollectionUtils.isEmpty(blocks)) {
            for (Block block : blocks) {
                if (!block.isComplete()) {
                    currentBlock = block;
                    break;
                }
            }
        }

        return currentBlock;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public Status getStatus() {
        return getCurrentBlock().getStatus();
    }

    @Override
    public boolean isComplete() {
        for (Block block : blocks) {
            if (!block.isComplete()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public PhaseStrategy getStrategy() {
        return strategy;
    }
}
