package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.offer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PersistentOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PersistentOfferRequirementProvider.class);
    private IdentityManager identityManager;
    private ConfigurationManager configurationManager;
    private CassandraTasks cassandraTasks;

    @Inject
    public PersistentOfferRequirementProvider(
            IdentityManager identityManager,
            ConfigurationManager configurationManager,
            CassandraTasks cassandraTasks) {
        this.identityManager = identityManager;
        this.configurationManager = configurationManager;
        this.cassandraTasks = cassandraTasks;
    }

    public OfferRequirement getNewOfferRequirement(CassandraContainer container) {
        // TODO: Should we version configs ?
        LOGGER.info("Getting new offer requirement for:  ", container.getId());
        final PlacementStrategy placementStrategy =
                PlacementStrategyManager.getPlacementStrategy(
                        configurationManager,
                        cassandraTasks);

        Protos.TaskInfo daemonTaskInfo = container.getDaemonTask().getTaskInfo();
        final List<Protos.SlaveID> agentsToAvoid =
                placementStrategy.getAgentsToAvoid(daemonTaskInfo);
        final List<Protos.SlaveID> agentsToColocate =
                placementStrategy.getAgentsToColocate(daemonTaskInfo);

        LOGGER.info("Avoiding agents: {}", agentsToAvoid);
        LOGGER.info("Colocating with agents: {}", agentsToColocate);

        final VolumeRequirement volumeRequirement = VolumeRequirement.create();
        volumeRequirement.setVolumeMode(VolumeRequirement.VolumeMode.CREATE);
        volumeRequirement.setVolumeType(configurationManager.getCassandraConfig().getDiskType());

        try {
            return new OfferRequirement(
                    container.getTaskInfos(),
                    container.getExecutorInfo(),
                    agentsToAvoid,
                    agentsToColocate);
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }

    public OfferRequirement getReplacementOfferRequirement(Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting replacement requirement for task: {}", taskInfo.getTaskId().getValue());
        return getExistingOfferRequirement(taskInfo);
    }

    public OfferRequirement getUpdateOfferRequirement(Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting updated requirement for task: {}", taskInfo.getTaskId().getValue());
        return getExistingOfferRequirement(taskInfo);
    }

    private OfferRequirement getExistingOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);
        final Identity identity = identityManager.get();
        final VolumeRequirement volumeRequirement = VolumeRequirement.create();
        volumeRequirement.setVolumeMode(VolumeRequirement.VolumeMode.EXISTING);
        volumeRequirement.setVolumeType(configurationManager.getCassandraConfig().getDiskType());

        ExecutorInfo execInfo = taskInfo.getExecutor();
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
