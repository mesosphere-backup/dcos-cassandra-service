package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.PlacementStrategy;
import org.apache.mesos.offer.VolumeRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ClusterTaskOfferRequirementProvider
        implements CassandraOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterTaskOfferRequirementProvider.class);
    private IdentityManager identityManager;
    private CassandraTasks cassandraTasks;
    private static VolumeRequirement MODE_NONE_TYPE_ROOT = VolumeRequirement.create();

    @Inject
    public ClusterTaskOfferRequirementProvider(
            IdentityManager identityManager,
            CassandraTasks cassandraTasks) {
        this.identityManager = identityManager;
        this.cassandraTasks = cassandraTasks;
    }

    @Override
    public OfferRequirement getNewOfferRequirement(Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting new offer requirement for nodeId: {}",
                taskInfo.getTaskId());
        return getCreateOfferRequirement(taskInfo);
    }

    private OfferRequirement getCreateOfferRequirement(Protos.TaskInfo taskInfo) {
        final PlacementStrategy placementStrategy = new ClusterTaskPlacementStrategy(
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
        return new OfferRequirement(
                identity.getRole(),
                identity.getPrincipal(),
                Arrays.asList(taskInfo),
                agentsToAvoid,
                agentsToColocate,
                MODE_NONE_TYPE_ROOT,
                OfferRequirement.ExecutorMode.EXISTING
        );
    }

    @Override
    public OfferRequirement getReplacementOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting replacement requirement for task: {}",
                taskInfo.getTaskId().getValue());
        final PlacementStrategy placementStrategy =
                new ClusterTaskPlacementStrategy(cassandraTasks);
        return new OfferRequirement(
                Arrays.asList(taskInfo),
                placementStrategy.getAgentsToAvoid(taskInfo),
                placementStrategy.getAgentsToColocate(taskInfo),
                MODE_NONE_TYPE_ROOT,
                OfferRequirement.ExecutorMode.EXISTING);
    }

    @Override
    public OfferRequirement getUpdateOfferRequirement(Protos.TaskInfo taskInfo) {
        return getExistingOfferRequirement(taskInfo);
    }

    private OfferRequirement getExistingOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);
        final Identity identity = identityManager.get();
        return new OfferRequirement(
                identity.getRole(),
                identity.getPrincipal(),
                Arrays.asList(taskInfo),
                null,
                null,
                MODE_NONE_TYPE_ROOT,
                OfferRequirement.ExecutorMode.EXISTING);
    }
}
