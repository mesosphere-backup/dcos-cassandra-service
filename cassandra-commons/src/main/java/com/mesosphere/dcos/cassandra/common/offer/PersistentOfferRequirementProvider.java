package com.mesosphere.dcos.cassandra.common.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.constrain.PlacementRuleGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PersistentOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PersistentOfferRequirementProvider.class);
    private DefaultConfigurationManager configurationManager;
    private CassandraState cassandraState;
    public static final String CONFIG_TARGET_KEY = "config_target";

    @Inject
    public PersistentOfferRequirementProvider(
            DefaultConfigurationManager configurationManager,
            CassandraState cassandraState) {
        this.configurationManager = configurationManager;
        this.cassandraState = cassandraState;
    }

    public Optional<OfferRequirement> getNewOfferRequirement(CassandraContainer container) {
        // TODO: Should we version configs ?
        LOGGER.info("Getting new offer requirement for: ", container.getId());

        Protos.TaskInfo daemonTaskInfo = container.getDaemonTask().getTaskInfo();

        Optional<PlacementRuleGenerator> placement = Optional.empty();
        try {
            placement = PlacementStrategyManager.getPlacementStrategy(
                    daemonTaskInfo,
                    configurationManager,
                    cassandraState);
        } catch (ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return Optional.empty();
        }

        try {
            final Collection<Protos.TaskInfo> taskInfos = container.getTaskInfos();
            final UUID targetName = configurationManager.getTargetName();
            final Collection<Protos.TaskInfo> updatedTaskInfos = updateConfigLabel(taskInfos, targetName.toString());

            return Optional.of(new OfferRequirement(
                    container.getDaemonTask().getType().name(),
                    clearTaskIds(updatedTaskInfos),
                    Optional.of(clearExecutorId(container.getExecutorInfo())),
                    placement));
        } catch (InvalidRequirementException | ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return Optional.empty();
        }
    }

    private Collection<Protos.TaskInfo> updateConfigLabel(Collection<Protos.TaskInfo> taskInfos, String configName) {
        Collection<Protos.TaskInfo> updatedTaskInfos = new ArrayList<>();
        for (Protos.TaskInfo taskInfo : taskInfos) {
            final Protos.TaskInfo updatedTaskInfo = updateConfigLabel(configName, taskInfo);
            updatedTaskInfos.add(updatedTaskInfo);
        }
        return updatedTaskInfos;
    }

    private Protos.TaskInfo updateConfigLabel(String configName, Protos.TaskInfo taskInfo) {
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

    public Optional<OfferRequirement> getReplacementOfferRequirement(CassandraContainer container) {
        LOGGER.info("Getting replacement requirement for task: {}", container.getId());
        try {
            return Optional.of(new OfferRequirement(
                    container.getDaemonTask().getType().name(),
                    clearTaskIds(container.getTaskInfos()),
                    Optional.of(clearExecutorId(container.getExecutorInfo())),
                    Optional.empty()));
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return Optional.empty();
        }
    }

    public OfferRequirement getUpdateOfferRequirement(String type, Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting updated requirement for task: {}", taskInfo.getTaskId());
        try {
            taskInfo = updateConfigLabel(configurationManager.getTargetName().toString(), taskInfo);
            return getExistingOfferRequirement(type, taskInfo);
        } catch (ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }

    private List<Protos.TaskInfo> clearTaskIds(Collection<Protos.TaskInfo> taskInfos) {
        List<Protos.TaskInfo> outTaskInfos = new ArrayList<>();
        for (Protos.TaskInfo restartTaskInfo : taskInfos) {
            outTaskInfos.add(
                    Protos.TaskInfo.newBuilder(restartTaskInfo)
                            .setTaskId(Protos.TaskID.newBuilder().setValue(""))
                            .build());
        }
        return outTaskInfos;
    }

    private ExecutorInfo clearExecutorId(ExecutorInfo executorInfo) {
        return ExecutorInfo.newBuilder(executorInfo)
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("").build())
                .build();
    }

    private OfferRequirement getExistingOfferRequirement(String type, Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);

        ExecutorInfo execInfo = clearExecutorId(taskInfo.getExecutor());
        LOGGER.info("ExecutorInfo: ", execInfo);
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(
                    type,
                    Arrays.asList(taskInfo),
                    Optional.of(execInfo),
                    Optional.empty());
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }
}
