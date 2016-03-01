package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.eventbus.EventBus;
import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.scheduler.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraDaemonPhase implements Phase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CassandraDaemonPhase.class);

    public static final CassandraDaemonPhase create(
            final ConfigurationManager configurationManager,
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {
        return new CassandraDaemonPhase(
                configurationManager,
                offerRequirementProvider,
                cassandraTasks,
                client);
    }

    private List<Block> blocks = null;
    private final int servers;
    private final CassandraOfferRequirementProvider offerRequirementProvider;
    private final CassandraTasks cassandraTasks;
    private final ExecutorClient client;
    private final PhaseStrategy strategy = new DefaultInstallStrategy(this);

    public CassandraDaemonPhase(
            final ConfigurationManager configurationManager,
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {
        this.servers = configurationManager.getServers();
        this.offerRequirementProvider = offerRequirementProvider;
        this.cassandraTasks = cassandraTasks;
        this.client = client;
        this.blocks = createBlocks();
    }

    private List<Block> createBlocks() {
        final List<Block> blocks = new ArrayList<>(servers);

        final List<String> names = new ArrayList<>(servers);

        for(int id = 0; id < servers; ++id){
            names.add(CassandraDaemonTask.NAME_PREFIX + id);
        }

        //here we will add a block for all tasks we have recorded and create a
        //new block with a newly recorded task for a scale out
        try {
            for (int i = 0; i < servers; i++) {
                final CassandraDaemonBlock daemonBlock =
                        CassandraDaemonBlock.create(i,
                                names.get(i),
                                offerRequirementProvider,
                                cassandraTasks,
                                this.client);
                blocks.add(daemonBlock);
                Collections.sort(blocks, (block1, block2) -> {
                    return Integer.compare(block1.getId(), block2.getId());
                });
            }
        } catch (Throwable throwable) {

            String message = "Failed to create CassandraDaemonPhase this is a" +
                    " fatal exception and the program will now exit. Please " +
                    "verify your scheduler configuration and attempt to " +
                    "relaunch the program.";

            LOGGER.error(message, throwable);

            throw new IllegalStateException(message, throwable);

        }

        return blocks;
    }

    @Override
    public List<? extends Block> getBlocks() {
        return blocks;
    }


    @Override
    public int getId() {
        return 0;
    }

    @Override
    public String getName() {
        return "CASSANDRA_DEPLOY";
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
}
