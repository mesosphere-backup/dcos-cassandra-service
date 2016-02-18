package com.mesosphere.dcos.cassandra.scheduler.backup;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OperationRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupOperationRecorder implements OperationRecorder {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupOperationRecorder.class);
    private BackupStateStore backupStateStore;

    public BackupOperationRecorder(BackupStateStore stateStore) {
        this.backupStateStore = stateStore;
    }

    @Override
    public void record(Protos.Offer.Operation operation, Protos.Offer offer)
            throws Exception {
        if (operation.getType() == Protos.Offer.Operation.Type.LAUNCH) {
            operation.getLaunch().getTaskInfosList().stream().forEach(
                    taskInfo -> {
                        LOGGER.debug("Recording operation: {} for task: {}",
                                operation,
                                taskInfo);
                        try {
                            backupStateStore.store(
                                    taskInfo.getTaskId().getValue()
                                    , offer);
                        } catch (Throwable throwable) {
                            LOGGER.error(String.format(
                                    "Error updating task in recorder: " +
                                            "operation = %s, " +
                                            "task = %s",
                                    operation,
                                    taskInfo));
                        }
                    });
        }
    }
}
