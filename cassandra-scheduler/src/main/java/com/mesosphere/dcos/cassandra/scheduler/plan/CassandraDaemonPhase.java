package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class CassandraDaemonPhase extends DefaultPhase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CassandraDaemonPhase.class);

    private static List<CassandraDaemonBlock> createBlocks(
            final ConfigurationManager configurationManager,
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {

        final int servers = configurationManager.getServers();

        final List<CassandraDaemonBlock> blocks = new ArrayList<>(servers);

        final List<String> names = new ArrayList<>(servers);

        for (int id = 0; id < servers; ++id) {
            names.add(CassandraDaemonTask.NAME_PREFIX + id);
        }

        Collections.sort(names);

        //here we will add a block for all tasks we have recorded and create a
        //new block with a newly recorded task for a scale out
        try {
            for (int i = 0; i < servers; i++) {
                final CassandraDaemonBlock daemonBlock =
                        CassandraDaemonBlock.create(
                                names.get(i),
                                offerRequirementProvider,
                                cassandraTasks,
                                client);
                blocks.add(daemonBlock);
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

    public static final CassandraDaemonPhase create(
            final ConfigurationManager configurationManager,
            final CassandraOfferRequirementProvider offerRequirementProvider,
            final CassandraTasks cassandraTasks,
            final ExecutorClient client) {
        return new CassandraDaemonPhase(
                createBlocks(
                        configurationManager,
                        offerRequirementProvider,
                        cassandraTasks,
                        client
                ));
    }


    public CassandraDaemonPhase( final List<CassandraDaemonBlock> blocks) {
        super(UUID.randomUUID(), "CassandraDeamonPhase", blocks);
    }

}
