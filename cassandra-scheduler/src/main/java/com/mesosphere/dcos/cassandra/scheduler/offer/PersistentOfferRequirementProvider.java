package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.scheduler.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.PlacementStrategy;
import org.apache.mesos.protobuf.LabelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PersistentOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PersistentOfferRequirementProvider.class);
    private DefaultConfigurationManager configurationManager;
    private CassandraTasks cassandraTasks;
    public static final String CONFIG_TARGET_KEY = "config_target";

    @Inject
    public PersistentOfferRequirementProvider(
            DefaultConfigurationManager configurationManager,
            CassandraTasks cassandraTasks) {
        this.configurationManager = configurationManager;
        this.cassandraTasks = cassandraTasks;
    }

    public OfferRequirement getNewOfferRequirement(CassandraContainer container) {
        // TODO: Should we version configs ?
        LOGGER.info("Getting new offer requirement for:  ", container.getId());
        PlacementStrategy placementStrategy;
        try {
            placementStrategy = PlacementStrategyManager.getPlacementStrategy(
                    configurationManager,
                    cassandraTasks);
        } catch (ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }

        Protos.TaskInfo daemonTaskInfo = container.getDaemonTask().getTaskInfo();
        final List<Protos.SlaveID> agentsToAvoid =
                placementStrategy.getAgentsToAvoid(daemonTaskInfo);
        final List<Protos.SlaveID> agentsToColocate =
                placementStrategy.getAgentsToColocate(daemonTaskInfo);

        LOGGER.info("Avoiding agents: {}", agentsToAvoid);
        LOGGER.info("Colocating with agents: {}", agentsToColocate);

        try {
            final Collection<Protos.TaskInfo> taskInfos = container.getTaskInfos();
            final UUID targetName = configurationManager.getTargetName();
            final Collection<Protos.TaskInfo> updatedTaskInfos = updateConfigLabel(taskInfos, targetName.toString());

            return new OfferRequirement(
                    updatedTaskInfos,
                    container.getExecutorInfo(),
                    agentsToAvoid,
                    agentsToColocate);
        } catch (InvalidRequirementException | ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
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
        final LabelBuilder labelBuilder = new LabelBuilder();

        final Protos.Labels labels = taskInfo.getLabels();
        for (Protos.Label label : labels.getLabelsList()) {
            final String key = label.getKey();
            if (!CONFIG_TARGET_KEY.equals(key)) {
                labelBuilder.addLabel(key, label.getValue());
            }
        }

        labelBuilder.addLabel(CONFIG_TARGET_KEY, configName);
        return Protos.TaskInfo.newBuilder(taskInfo)
                .clearLabels()
                .setLabels(labelBuilder.build())
                .build();
    }

    public OfferRequirement getReplacementOfferRequirement(Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting replacement requirement for task: {}", taskInfo.getTaskId().getValue());
        return getExistingOfferRequirement(taskInfo);
    }

    public OfferRequirement getUpdateOfferRequirement(Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting updated requirement for task: {}", taskInfo.getTaskId());
        try {
            taskInfo = updateConfigLabel(configurationManager
                    .getTargetName().toString(), taskInfo);
            return getExistingOfferRequirement(taskInfo);
        } catch (ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }

    private OfferRequirement getExistingOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);

        final ExecutorInfo execInfo = ExecutorInfo.newBuilder(taskInfo.getExecutor())
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(""))
                .build();
        LOGGER.info("ExecutorInfo: ", execInfo);
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(
                    Arrays.asList(taskInfo),
                    execInfo,
                    null,
                    null);
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }
}
