package com.mesosphere.dcos.cassandra.common.offer;

import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

public class ClusterTaskOfferRequirementProvider implements CassandraOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterTaskOfferRequirementProvider.class);

    @Inject
    public ClusterTaskOfferRequirementProvider() {
    }

    @Override
    public OfferRequirement getNewOfferRequirement(String type, Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting new offer requirement for nodeId: {}",
                taskInfo.getTaskId());
        return getCreateOfferRequirement(type, taskInfo);
    }

    private OfferRequirement getCreateOfferRequirement(String type, Protos.TaskInfo taskInfo) {
        ExecutorInfo execInfo = taskInfo.getExecutor();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(
                    type,
                    Arrays.asList(taskInfo),
                    Optional.of(execInfo));
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }

    @Override
    public OfferRequirement getReplacementOfferRequirement(String type, Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting replacement requirement for task: {}",
                taskInfo.getTaskId().getValue());

        ExecutorInfo execInfo = taskInfo.getExecutor();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(
                    type,
                    Arrays.asList(taskInfo),
                    Optional.of(execInfo));
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }

    }

    @Override
    public OfferRequirement getUpdateOfferRequirement(String type, Protos.TaskInfo taskInfo) {
        return getExistingOfferRequirement(type, taskInfo);
    }

    private OfferRequirement getExistingOfferRequirement(String type, Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);

        ExecutorInfo execInfo = taskInfo.getExecutor();
        taskInfo = Protos.TaskInfo.newBuilder(taskInfo).clearExecutor().build();

        try {
            return new OfferRequirement(type, Arrays.asList(taskInfo), Optional.of(execInfo));
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }
}
