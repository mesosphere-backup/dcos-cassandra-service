package com.mesosphere.dcos.cassandra.common.offer;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.OperationRecorder;
import org.apache.mesos.offer.TaskUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentOperationRecorder implements OperationRecorder {
    private final static Logger LOGGER = LoggerFactory.getLogger(
            PersistentOperationRecorder.class);
    private CassandraState cassandraState;
    public PersistentOperationRecorder(
            CassandraState cassandraState) {
        this.cassandraState = cassandraState;
    }

    public void record(
            Protos.Offer.Operation operation,
            Protos.Offer offer) throws Exception {
        if (operation.getType() == Protos.Offer.Operation.Type.LAUNCH) {
            LOGGER.info("Persisting Launch Operation: " + operation);
            for (TaskInfo taskInfo : operation.getLaunch().getTaskInfosList()) {
                LOGGER.debug("Recording operation: {} for task: {}", operation, taskInfo);
                try {
                    cassandraState.update(TaskUtils.unpackTaskInfo(taskInfo), offer);
                } catch (Exception e) {
                    LOGGER.error("Error updating task in recorder with exception: ", e);
                    throw e;
                }
            }
        }
    }
}
