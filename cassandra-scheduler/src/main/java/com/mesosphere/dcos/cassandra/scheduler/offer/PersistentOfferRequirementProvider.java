package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.PlacementStrategy;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.VolumeRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PersistentOfferRequirementProvider implements CassandraOfferRequirementProvider {
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

    @Override
    public OfferRequirement getNewOfferRequirement(Protos.TaskInfo taskInfo) {
        // TODO: Should we version configs ?
        LOGGER.info("Getting new offer requirement for nodeId: task", taskInfo);
        return getCreateOfferRequirement(taskInfo);
    }

    private OfferRequirement getCreateOfferRequirement(Protos.TaskInfo taskInfo) {
        final PlacementStrategy placementStrategy =
                PlacementStrategyManager.getPlacementStrategy(
                        configurationManager,
                        cassandraTasks);
        final List<Protos.SlaveID> agentsToAvoid =
                placementStrategy.getAgentsToAvoid(
                        taskInfo);
        final List<Protos.SlaveID> agentsToColocate =
                placementStrategy.getAgentsToColocate(
                        taskInfo);

        LOGGER.info("Avoiding agents: {}", agentsToAvoid);
        LOGGER.info("Colocating with agents: {}", agentsToColocate);
        final Identity identity = identityManager.get();
        final VolumeRequirement volumeRequirement = VolumeRequirement.create();
        volumeRequirement.setVolumeMode(VolumeRequirement.VolumeMode.CREATE);
        volumeRequirement.setVolumeType(configurationManager.getCassandraConfig().getDiskType());
        return new OfferRequirement(
                identity.getRole(),
                identity.getPrincipal(),
                Arrays.asList(taskInfo),
                agentsToAvoid,
                agentsToColocate,
                volumeRequirement
        );
    }

    @Override
    public OfferRequirement getReplacementOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting replacement requirement for task: {}", taskInfo.getTaskId().getValue());
        if (hasVolume(taskInfo)) {
            LOGGER.info("Task has a volume, taskId: {}, reusing existing requirement", taskInfo.getTaskId().getValue());
            return getExistingOfferRequirement(taskInfo);
        } else {
            LOGGER.info("Task doesn't has a volume, taskId: {}, creating a new requirement", taskInfo.getTaskId().getValue());
            final PlacementStrategy placementStrategy =
                    PlacementStrategyManager.getPlacementStrategy(
                            configurationManager,
                            cassandraTasks);

            return new OfferRequirement(
                    Arrays.asList(taskInfo),
                    placementStrategy.getAgentsToAvoid(taskInfo),
                    placementStrategy.getAgentsToColocate(taskInfo));
        }
    }

    @Override
    public OfferRequirement getUpdateOfferRequirement(Protos.TaskInfo taskInfo) {
        if (hasVolume(taskInfo)) {
            LOGGER.info("Task has a volume, taskId: {}, reusing existing requirement", taskInfo.getTaskId().getValue());
            return getExistingOfferRequirement(taskInfo);
        } else {
            LOGGER.info("Task doesn't has a volume, taskId: {}, creating a new requirement", taskInfo.getTaskId().getValue());
            return getCreateOfferRequirement(taskInfo);
        }
    }

    private OfferRequirement getExistingOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);
        final Identity identity = identityManager.get();
        final VolumeRequirement volumeRequirement = VolumeRequirement.create();
        volumeRequirement.setVolumeMode(VolumeRequirement.VolumeMode.EXISTING);
        volumeRequirement.setVolumeType(configurationManager.getCassandraConfig().getDiskType());
        return new OfferRequirement(
                identity.getRole(),
                identity.getPrincipal(),
                Arrays.asList(taskInfo),
                null,
                null,
                volumeRequirement);
    }

    private boolean hasVolume(Protos.TaskInfo taskInfo) {
        String containerPath = ResourceUtils.getVolumeContainerPath(
                taskInfo.getResourcesList());
        return containerPath != null;
    }
}
