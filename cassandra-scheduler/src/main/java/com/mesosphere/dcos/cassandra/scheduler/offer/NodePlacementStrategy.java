package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.PlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NodePlacementStrategy implements PlacementStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            NodePlacementStrategy.class);

    private CassandraTasks cassandraTasks;

    public NodePlacementStrategy(CassandraTasks cassandraTasks) {
        this.cassandraTasks = cassandraTasks;
    }

    @Override
    public List<Protos.SlaveID> getAgentsToAvoid(Protos.TaskInfo taskInfo) {
        List<Protos.SlaveID> agentsToAvoid = new ArrayList<>();
        final List<Protos.TaskInfo> otherTaskInfos = getOtherTaskInfos(
                taskInfo);
        otherTaskInfos.stream().forEach(otherTaskInfo ->
                agentsToAvoid.add(otherTaskInfo.getSlaveId()));
        LOGGER.debug("Avoiding agents: {}", agentsToAvoid);
        return agentsToAvoid;
    }

    @Override
    public List<Protos.SlaveID> getAgentsToColocate(Protos.TaskInfo taskInfo) {
        return null;
    }

    private List<Protos.TaskInfo> getOtherTaskInfos(Protos.TaskInfo thisTaskInfo) {
        final Protos.TaskID taskId = thisTaskInfo.getTaskId();
        final String taskIdValue = taskId.getValue();
        final Map<String, CassandraDaemonTask> daemons = cassandraTasks.getDaemons();

        final List<Protos.TaskInfo> others = daemons.values().stream()
                .filter(task -> !task.getId().equals(taskIdValue))
                .map(task -> task.toProto()).collect(Collectors.toList());

        return others;
    }
}
