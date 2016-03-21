package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OperationRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentOperationRecorder implements OperationRecorder {
    private final static Logger LOGGER = LoggerFactory.getLogger(
            PersistentOperationRecorder.class);

    private CassandraTasks cassandraTasks;

    public PersistentOperationRecorder(CassandraTasks cassandraTasks) {
        this.cassandraTasks = cassandraTasks;
    }

    public void record(
            Protos.Offer.Operation operation,
            Protos.Offer offer) throws Exception {
        if (operation.getType() == Protos.Offer.Operation.Type.LAUNCH) {
            operation.getLaunch().getTaskInfosList().stream().forEach(
                    taskInfo -> {
                        LOGGER.debug("Recording operation: {} for task: {}",
                                operation,
                                taskInfo);
                        try {
                            cassandraTasks.update(
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
