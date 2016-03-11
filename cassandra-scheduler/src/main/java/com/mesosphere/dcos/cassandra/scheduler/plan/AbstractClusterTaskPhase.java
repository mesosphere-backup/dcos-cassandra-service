package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public abstract class AbstractClusterTaskPhase<B extends Block, C extends ClusterTaskContext> implements Phase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractClusterTaskPhase.class);

    protected final UUID id = UUID.randomUUID();
    protected final C context;
    protected final List<B> blocks;
    protected final CassandraTasks cassandraTasks;
    protected final ClusterTaskOfferRequirementProvider provider;

    public AbstractClusterTaskPhase(
            C context,
            CassandraTasks cassandraTasks,
            ClusterTaskOfferRequirementProvider provider) {
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
    public UUID getId() {
        return id;
    }

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
    public Block getBlock(UUID id) {
        for (Block block : getBlocks()) {
            if (block.getId().equals(id)) {
                return block;
            }
        }
        return null;
    }

    @Override
    public Block getBlock(int index) {
        List<? extends Block> blocks = getBlocks();
        if (index < blocks.size()) {
            return blocks.get(index);
        }
        return null;
    }
}
