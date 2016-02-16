package com.mesosphere.dcos.cassandra.common.util;

import org.apache.mesos.Protos;

public class TaskUtils {
    public static int taskIdToNodeId(String taskId) {
        final int start = taskId.indexOf("-");
        final int end = taskId.indexOf("_");
        return Integer.parseInt(taskId.substring(start + 1, end));
    }

    public static String getPersistenceId(Protos.TaskInfo taskInfo) {
        for (Protos.Resource resource : taskInfo.getResourcesList()) {
            if (resource.getName().equals("disk")) {
                Protos.Resource.DiskInfo diskInfo = resource.getDisk();
                if (diskInfo != null) {
                    Protos.Resource.DiskInfo.Persistence persistence = diskInfo.getPersistence();
                    return persistence.getId();
                }
            }
        }

        return null;
    }
}
