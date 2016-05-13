package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.google.common.collect.ImmutableList;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
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

    private static void createBlocks(
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final CassandraOfferRequirementProvider provider,
            final SchedulerClient client,
            final List<CassandraDaemonBlock> blocks,
            final List<String> errors) {

        final int servers = configurationManager.getServers();

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
                                provider,
                                cassandraTasks,
                                client);
                blocks.add(daemonBlock);
            }
        } catch (Throwable throwable) {
            LOGGER.error("Error creating CassandraDaemonBlock", throwable);
            errors.add(String.format(
                    "Error creating CassandraDaemonBlock : message = %s",
                    throwable.getMessage()));

        }
    }

    private final List<String> errors;

    public static final CassandraDaemonPhase create(
            final ConfigurationManager configurationManager,
            final CassandraTasks cassandraTasks,
            final CassandraOfferRequirementProvider provider,
            final SchedulerClient client) {

        final List<CassandraDaemonBlock> blocks =
                new ArrayList<>();

        final List<String> errors = new ArrayList<>();
        createBlocks(
                configurationManager,
                cassandraTasks,
                provider,
                client,
                blocks,
                errors
        );
        return new CassandraDaemonPhase(blocks, errors);
    }


    public CassandraDaemonPhase(
            final List<CassandraDaemonBlock> blocks,
            final List<String> errors) {
        super(UUID.randomUUID(), "Deploy", blocks);
        this.errors = errors;
    }

    public List<String> getErrors() {
        return ImmutableList.copyOf(errors);
    }

}
