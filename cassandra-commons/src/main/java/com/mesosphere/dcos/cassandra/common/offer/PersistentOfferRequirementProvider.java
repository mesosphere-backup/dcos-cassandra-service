package com.mesosphere.dcos.cassandra.common.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.placementrule.AvailiabilityZonePlacementRule;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.constrain.AndRule;
import org.apache.mesos.offer.constrain.MarathonConstraintParser;
import org.apache.mesos.offer.constrain.PassthroughRule;
import org.apache.mesos.offer.constrain.PlacementRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class PersistentOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PersistentOfferRequirementProvider.class);
    private DefaultConfigurationManager configurationManager;
    public static final String CONFIG_TARGET_KEY = "config_target";
    
    private static Map<String, String> nodeToZoneCodeMap;
    
    @Inject
    public PersistentOfferRequirementProvider(DefaultConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public Optional<OfferRequirement> getNewOfferRequirement(CassandraContainer container) {
        LOGGER.info("Getting new offer requirement for: ", container.getId());

        LOGGER.info("Nodes to zone map: {}", nodeToZoneCodeMap.toString());
        Optional<PlacementRule> placementRule = Optional.empty();
        try {
            placementRule = getPlacementRule();
        } catch (IOException e) {
            LOGGER.error("Failed to construct PlacementRule with Exception: ", e);
            return Optional.empty();
        }

        try {
            final Collection<Protos.TaskInfo> taskInfos = container.getTaskInfos();
            final UUID targetName = configurationManager.getTargetName();
            final Collection<Protos.TaskInfo> updatedTaskInfos = updateConfigLabel(taskInfos, targetName.toString());
            
			if (nodeToZoneCodeMap != null && !nodeToZoneCodeMap.isEmpty()) {
				String zone = nodeToZoneCodeMap.get(container.getDaemonTask().getName());
				LOGGER.info("Zone : {}", zone);
				if (zone != null) {
					Optional<PlacementRule> availabilityZonePlacementRule = getAvailiabiltyZonePlacementRule(zone);
					placementRule = availabilityZonePlacementRule;// mergeRules(placementRule,
																	// availabilityZonePlacementRule);
				}
			}
            
            return Optional.of(OfferRequirement.create(
                    container.getDaemonTask().getType().name(),
                    clearTaskIds(updatedTaskInfos),
                    Optional.of(clearExecutorId(container.getExecutorInfo())),
                    placementRule));
        } catch (InvalidRequirementException | ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return Optional.empty();
        } catch (IOException e) {
			LOGGER.error("Failed to construct OfferRequirement with Exception, zone issue: ", e);
			return Optional.empty();
		}
    }

    public Optional<OfferRequirement> getReplacementOfferRequirement(CassandraContainer container) {
        LOGGER.info("Getting replacement requirement for task: {}", container.getId());
        try {
            return Optional.of(OfferRequirement.create(
                    container.getDaemonTask().getType().name(),
                    clearTaskIds(container.getTaskInfos()),
                    Optional.of(clearExecutorId(container.getExecutorInfo())),
                    Optional.empty()));
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return Optional.empty();
        }
    }

	private Optional<PlacementRule> getAvailiabiltyZonePlacementRule(String az) throws IOException {
		return Optional.of(new AvailiabilityZonePlacementRule(az));
	}
	
	private Optional<PlacementRule> mergeRules(Optional<PlacementRule> rule1, Optional<PlacementRule> rule2) {
		AndRule andRule = new AndRule(rule1.get(), rule2.get());
		return Optional.of(andRule);
	}
    private Optional<PlacementRule> getPlacementRule() throws IOException {
        // Due to all cassandra nodes always requiring the same port, they will never colocate.
        // Therefore we don't bother with explicit node avoidance in placement rules here.
        // If we did want to enforce that here, we'd use PlacementUtils.getAgentPlacementRule(), and
        // merge the result with the marathon placement (if any) using MarathonConstraintParser.parseWith().
        String marathonPlacement =
                ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getPlacementConstraint();
        PlacementRule rule = MarathonConstraintParser.parse(marathonPlacement);
        if (rule instanceof PassthroughRule) {
            LOGGER.info("No placement constraints found for marathon-style constraint '{}': {}", marathonPlacement, rule);
            return Optional.empty();
        }
        LOGGER.info("Created placement rule for marathon-style constraint '{}': {}", marathonPlacement, rule);
        return Optional.of(rule);
    }

    private static Collection<Protos.TaskInfo> updateConfigLabel(Collection<Protos.TaskInfo> taskInfos, String configName) {
        Collection<Protos.TaskInfo> updatedTaskInfos = new ArrayList<>();
        for (Protos.TaskInfo taskInfo : taskInfos) {
            final Protos.TaskInfo updatedTaskInfo = updateConfigLabel(configName, taskInfo);
            updatedTaskInfos.add(updatedTaskInfo);
        }
        return updatedTaskInfos;
    }

    private static Protos.TaskInfo updateConfigLabel(String configName, Protos.TaskInfo taskInfo) {
        final Protos.Labels.Builder labelsBuilder = Protos.Labels.newBuilder();

        final Protos.Labels labels = taskInfo.getLabels();
        for (Protos.Label label : labels.getLabelsList()) {
            final String key = label.getKey();
            if (!CONFIG_TARGET_KEY.equals(key)) {
                labelsBuilder.addLabels(label);
            }
        }

        labelsBuilder.addLabels(Protos.Label.newBuilder()
                .setKey(CONFIG_TARGET_KEY)
                .setValue(configName));
        return Protos.TaskInfo.newBuilder(taskInfo)
                .clearLabels()
                .setLabels(labelsBuilder.build())
                .build();
    }

    private static List<Protos.TaskInfo> clearTaskIds(Collection<Protos.TaskInfo> taskInfos) {
        List<Protos.TaskInfo> outTaskInfos = new ArrayList<>();
        for (Protos.TaskInfo restartTaskInfo : taskInfos) {
            outTaskInfos.add(
                    Protos.TaskInfo.newBuilder(restartTaskInfo)
                            .setTaskId(Protos.TaskID.newBuilder().setValue(""))
                            .build());
        }
        return outTaskInfos;
    }

    private static ExecutorInfo clearExecutorId(ExecutorInfo executorInfo) {
        return ExecutorInfo.newBuilder(executorInfo)
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("").build())
                .build();
    }
    
    public static void setNodeToZoneInformationMap(Map<String, String> nodeToZoneCodeMap){
		PersistentOfferRequirementProvider.nodeToZoneCodeMap = nodeToZoneCodeMap;
    }
}
