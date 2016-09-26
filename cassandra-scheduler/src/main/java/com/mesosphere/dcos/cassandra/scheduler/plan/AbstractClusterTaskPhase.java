package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.scheduler.DefaultObservable;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;

import java.util.List;
import java.util.UUID;

public abstract class AbstractClusterTaskPhase<B extends Block, C extends ClusterTaskContext>
        extends DefaultObservable
        implements Phase {

    protected final UUID id = UUID.randomUUID();
    protected final C context;
    protected final List<B> blocks;
    protected final CassandraState cassandraState;
    protected final ClusterTaskOfferRequirementProvider provider;

    public AbstractClusterTaskPhase(
            C context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        this.provider = provider;
        this.context = context;
        this.cassandraState = cassandraState;
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
