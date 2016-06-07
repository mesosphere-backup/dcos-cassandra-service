package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.mesosphere.dcos.cassandra.scheduler.config.CuratorFrameworkConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;
import com.mesosphere.dcos.cassandra.scheduler.config.MesosConfig;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.OperationRecorder;
import org.apache.mesos.state.CuratorStateStore;
import org.apache.mesos.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class PersistentOperationRecorder implements OperationRecorder {
    private final static Logger LOGGER = LoggerFactory.getLogger(
            PersistentOperationRecorder.class);

    private IdentityManager identityManager;
    private CassandraTasks cassandraTasks;
    private StateStore stateStore;

    public PersistentOperationRecorder(
            IdentityManager identityManager,
            CuratorFrameworkConfig curatorConfig,
            CassandraTasks cassandraTasks) {

        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        this.identityManager = identityManager;
        this.cassandraTasks = cassandraTasks;
        this.stateStore = new CuratorStateStore(
                identityManager.get().getName(),
                curatorConfig.getServers(),
                retryPolicy);
    }

    public void record(
            Protos.Offer.Operation operation,
            Protos.Offer offer) throws Exception {
        if (operation.getType() == Protos.Offer.Operation.Type.LAUNCH) {
            LOGGER.info("Persisting Launch Operation: " + operation);

            for (TaskInfo taskInfo : operation.getLaunch().getTaskInfosList()) {
                LOGGER.debug("Recording operation: {} for task: {}", operation, taskInfo);

                try {
                    stateStore.storeTasks(Arrays.asList(taskInfo), taskInfo.getExecutor().getExecutorId());
                    cassandraTasks.update(taskInfo, offer);
                } catch (Throwable throwable) {
                    LOGGER.error(String.format(
                            "Error updating task in recorder: " +
                                    "operation = %s, " +
                                    "task = %s",
                            operation,
                            taskInfo));
                }
            }
        }
    }
}
