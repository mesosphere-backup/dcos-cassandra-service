package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.collect.Lists;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.PlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ClusterTaskPlacementStrategy implements PlacementStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterTaskPlacementStrategy.class);

    private CassandraTasks cassandraTasks;

    public ClusterTaskPlacementStrategy(CassandraTasks cassandraTasks) {
        this.cassandraTasks = cassandraTasks;
    }

    @Override
    public List<Protos.SlaveID> getAgentsToAvoid(Protos.TaskInfo taskInfo) {
        return Lists.newArrayList();
    }

    @Override
    public List<Protos.SlaveID> getAgentsToColocate(Protos.TaskInfo taskInfo) {
        List<Protos.SlaveID> agentsToColocate = new ArrayList<>();
        // Colocate this task with the corresponding Cassandra node task
        final Optional<Protos.TaskInfo> matchingNodeTask = getMatchingNodeTask(
                taskInfo);
        if (matchingNodeTask.isPresent()) {
            agentsToColocate.add(matchingNodeTask.get().getSlaveId());
        }
        LOGGER.info("Colocating task: {} with agent: {}",
                taskInfo.getTaskId().getValue(), agentsToColocate);
        return agentsToColocate;
    }

    private Optional<Protos.TaskInfo> getMatchingNodeTask(Protos.TaskInfo thisTaskInfo) {
        final int nodeId = TaskUtils.taskIdToNodeId(thisTaskInfo.getTaskId().getValue());
        final Optional<CassandraDaemonTask> first = cassandraTasks.getDaemons().values().stream()
                .filter(task -> nodeId == TaskUtils.taskIdToNodeId(task.getId())).findFirst();

        return Optional.of(first.isPresent() ? first.get().toProto() : null);
    }
}
