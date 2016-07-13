package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.scheduler.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.scheduler.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.PlacementStrategy;
import org.apache.mesos.offer.VolumeRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PersistentOfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            PersistentOfferRequirementProvider.class);
    private IdentityManager identityManager;
    private DefaultConfigurationManager configurationManager;
    private CassandraTasks cassandraTasks;

    @Inject
    public PersistentOfferRequirementProvider(
            IdentityManager identityManager,
            DefaultConfigurationManager configurationManager,
            CassandraTasks cassandraTasks) {
        this.identityManager = identityManager;
        this.configurationManager = configurationManager;
        this.cassandraTasks = cassandraTasks;
    }

    public OfferRequirement getNewOfferRequirement(CassandraContainer container) {
        // TODO: Should we version configs ?
        LOGGER.info("Getting new offer requirement for:  ", container.getId());
        PlacementStrategy placementStrategy = null;
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

        final VolumeRequirement volumeRequirement = VolumeRequirement.create();
        volumeRequirement.setVolumeMode(VolumeRequirement.VolumeMode.CREATE);
        try {
            volumeRequirement.setVolumeType(((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                    .getCassandraConfig().getDiskType());
        } catch (ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }
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
        try {
            volumeRequirement.setVolumeType(((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                    .getCassandraConfig().getDiskType());
        } catch (ConfigStoreException e) {
            LOGGER.error("Failed to construct OfferRequirement with Exception: ", e);
            return null;
        }

        ExecutorInfo execInfo = ExecutorInfo.newBuilder(taskInfo.getExecutor())
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
