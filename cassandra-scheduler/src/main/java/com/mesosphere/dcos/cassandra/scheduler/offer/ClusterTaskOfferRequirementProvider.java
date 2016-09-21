package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.PlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ClusterTaskOfferRequirementProvider
        implements CassandraOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterTaskOfferRequirementProvider.class);

    @Inject
    public ClusterTaskOfferRequirementProvider() {
    }

    @Override
    public OfferRequirement getNewOfferRequirement(Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting new offer requirement for nodeId: {}",
                taskInfo.getTaskId());
        return getCreateOfferRequirement(taskInfo);
    }

    private OfferRequirement getCreateOfferRequirement(Protos.TaskInfo taskInfo) {
        final PlacementStrategy placementStrategy = new ClusterTaskPlacementStrategy();
        final List<Protos.SlaveID> agentsToAvoid =
                placementStrategy.getAgentsToAvoid(taskInfo);
        final List<Protos.SlaveID> agentsToColocate =
                placementStrategy.getAgentsToColocate(taskInfo);

        LOGGER.info("Avoiding agents: {}", agentsToAvoid);
        LOGGER.info("Colocating with agents: {}", agentsToColocate);

        ExecutorInfo execInfo = taskInfo.getExecutor();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(
                    Arrays.asList(taskInfo),
                    Optional.of(execInfo),
                    agentsToAvoid,
                    agentsToColocate);
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }

    @Override
    public OfferRequirement getReplacementOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting replacement requirement for task: {}",
                taskInfo.getTaskId().getValue());
        final PlacementStrategy placementStrategy =
                new ClusterTaskPlacementStrategy();

        ExecutorInfo execInfo = taskInfo.getExecutor();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(
                    Arrays.asList(taskInfo),
                    Optional.of(execInfo),
                    placementStrategy.getAgentsToAvoid(taskInfo),
                    placementStrategy.getAgentsToColocate(taskInfo));
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }

    }

    @Override
    public OfferRequirement getUpdateOfferRequirement(Protos.TaskInfo taskInfo) {
        return getExistingOfferRequirement(taskInfo);
    }

    private OfferRequirement getExistingOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);

        ExecutorInfo execInfo = taskInfo.getExecutor();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(
                    Arrays.asList(taskInfo),
                    Optional.of(execInfo),
                    null,
                    null);
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }
}
