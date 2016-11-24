package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CassandraDaemonPhase extends DefaultPhase {

    private static List<Step> createSteps(
            final CassandraState cassandraState,
            final PersistentOfferRequirementProvider provider,
            final DefaultConfigurationManager configurationManager)
                throws ConfigStoreException, IOException {
        final int servers = ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                .getServers();

        final List<String> names = new ArrayList<>(servers);

        for (int id = 0; id < servers; ++id) {
            names.add(CassandraDaemonTask.NAME_PREFIX + id);
        }

        Collections.sort(names);

        //here we will add a block for all tasks we have recorded and create a
        //new block with a newly recorded task for a scale out
        final List<Step> steps = new ArrayList<>();
        for (int i = 0; i < servers; i++) {
            steps.add(CassandraDaemonStep.create(names.get(i), provider, cassandraState));
        }
        return steps;
    }

    public static final CassandraDaemonPhase create(
            final CassandraState cassandraState,
            final PersistentOfferRequirementProvider provider,
            final SchedulerClient client,
            final DefaultConfigurationManager configurationManager)
                throws ConfigStoreException {
        try {
            return new CassandraDaemonPhase(
                    createSteps(cassandraState, provider, configurationManager),
                    new ArrayList<>());
        } catch (Throwable e) {
            return new CassandraDaemonPhase(new ArrayList<>(), Arrays.asList(String.format(
                    "Error creating CassandraDaemonBlock : message = %s", e.getMessage())));
        }
    }

    public CassandraDaemonPhase(
            final List<Step> steps,
            final List<String> errors) {
        super("Deploy", steps, new SerialStrategy<>(), errors);
    }
}
