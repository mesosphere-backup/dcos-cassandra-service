package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.PlacementStrategy;
import org.apache.mesos.offer.VolumeRequirement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
                    clearTaskIds(container.getTaskInfos()),
                    clearExecutorId(container.getExecutorInfo()),
                    agentsToAvoid,
                    agentsToColocate);
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }

    public OfferRequirement getReplacementOfferRequirement(CassandraContainer container) {
        LOGGER.info("Getting replacement requirement for task: {}", container.getId());
        try {
            return new OfferRequirement(
                    clearTaskIds(container.getTaskInfos()),
                    clearExecutorId(container.getExecutorInfo()),
                    null,
                    null);
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
    }

    public OfferRequirement getUpdateOfferRequirement(Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting updated requirement for task: {}", taskInfo.getTaskId().getValue());
        return getExistingOfferRequirement(taskInfo);
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

    private OfferRequirement getExistingOfferRequirement(
            Protos.TaskInfo taskInfo) {
        LOGGER.info("Getting existing OfferRequirement for task: {}", taskInfo);
        final Identity identity = identityManager.get();
        final VolumeRequirement volumeRequirement = VolumeRequirement.create();
        volumeRequirement.setVolumeMode(VolumeRequirement.VolumeMode.EXISTING);
        volumeRequirement.setVolumeType(configurationManager.getCassandraConfig().getDiskType());

        ExecutorInfo execInfo = clearExecutorId(taskInfo.getExecutor());
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
