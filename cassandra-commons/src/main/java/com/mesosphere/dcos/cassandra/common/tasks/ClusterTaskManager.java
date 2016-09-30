package com.mesosphere.dcos.cassandra.common.tasks;

import java.util.List;

import org.apache.mesos.scheduler.plan.Completable;
import org.apache.mesos.scheduler.plan.Phase;

/**
 * Interface for managers of ClusterTask execution (e.g Backup, Restore, Cleanup, ... )
 *
 * @param <Context> the {@link ClusterTaskContext} used by the implementing manager
 */
public interface ClusterTaskManager<R extends ClusterTaskRequest> extends Completable {

    public static boolean canStart(ClusterTaskManager<?> manager) {
        return !manager.isInProgress();
    }
    public static boolean canStop(ClusterTaskManager<?> manager) {
        return manager.isInProgress();
    }

    public void start(R request);
    public void stop();
    public boolean isInProgress();
    public boolean isComplete();
    public List<Phase> getPhases();
}
