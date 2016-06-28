package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.OperationRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentOperationRecorder implements OperationRecorder {
    private final static Logger LOGGER = LoggerFactory.getLogger(
            PersistentOperationRecorder.class);

    private IdentityManager identityManager;
    private CassandraTasks cassandraTasks;

    public PersistentOperationRecorder(
            IdentityManager identityManager,
            CassandraTasks cassandraTasks) {

        this.identityManager = identityManager;
        this.cassandraTasks = cassandraTasks;
    }

    public void record(
            Protos.Offer.Operation operation,
            Protos.Offer offer) throws Exception {
        if (operation.getType() == Protos.Offer.Operation.Type.LAUNCH) {
            LOGGER.info("Persisting Launch Operation: {}", TextFormat.shortDebugString(operation));

            for (TaskInfo taskInfo : operation.getLaunch().getTaskInfosList()) {
                LOGGER.debug("Recording operation: {} for task: {}", operation, taskInfo);

                try {
                    cassandraTasks.update(taskInfo, offer);
                } catch (Exception e) {
                    LOGGER.error("Error updating task in recorder with exception: ", e);
                    throw e;
                }
            }
        }
    }
}
