package com.mesosphere.dcos.cassandra.scheduler.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.CassandraScheduler;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;

public class CassandraDaemonPhase extends DefaultPhase {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);
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
        
        
		String commaSepratedZones = ((CassandraSchedulerConfiguration) configurationManager.getTargetConfig())
				.getZones();
		
		if (commaSepratedZones != null && !commaSepratedZones.isEmpty()) {
			String[] zones = commaSepratedZones.split(",");
			Map<Integer, String> nodeIndexToZoneCodeMap = getNodeToZoneMapWithZones(servers, zones);

			Map<String, String> nodeTaskNameToZoneCodeMap = new HashMap<String, String>();
			for (int i = 0; i < servers; i++) {
				nodeTaskNameToZoneCodeMap.put(names.get(i), nodeIndexToZoneCodeMap.get(i));
			}
			LOGGER.info("Node Index To Zone CodeMap :{}", nodeIndexToZoneCodeMap);
		}
		
        
		//Collections.sort(names);

        // here we will add a step for all tasks we have recorded and create a
        // new step with a newly recorded task for a scale out
		
		
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
            final DefaultConfigurationManager configurationManager) {
        try {
            return new CassandraDaemonPhase(
                    createSteps(cassandraState, provider, configurationManager),
                    new ArrayList<>());
        } catch (Throwable e) {
            return new CassandraDaemonPhase(new ArrayList<>(), Arrays.asList(String.format(
                    "Error creating CassandraDaemonStep : message = %s", e.getMessage())));
        }
    }

    public CassandraDaemonPhase(
            final List<Step> steps,
            final List<String> errors) {
        super("Deploy", steps, new SerialStrategy<>(), errors);
    }
    
	private static Map<Integer, String> getNodeToZoneMapWithZones(int servers, String[] zones) {

		int totalZones = zones.length;

		int serversOnEachZone = servers / totalZones;
		int remainingServers = servers % totalZones;

		int serverNumber = 0;
		Map<Integer, String> serverIndexToZoneMap = new HashMap<>();
		for (String zone : zones) {
			int serverCount = 0;
			while (serverCount < serversOnEachZone) {
				serverIndexToZoneMap.put(serverNumber, zone);
				serverCount++;
				serverNumber++;
			}
		}

		while (remainingServers > 0) {
			serverIndexToZoneMap.put(serverNumber, zones[remainingServers]);
			serverNumber++;
			remainingServers--;
		}

		return serverIndexToZoneMap;
	}
}
