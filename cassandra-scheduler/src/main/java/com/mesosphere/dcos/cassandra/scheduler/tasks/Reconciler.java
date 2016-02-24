package com.mesosphere.dcos.cassandra.scheduler.tasks;

import com.google.common.eventbus.Subscribe;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

public interface Reconciler {
    void start();

    void reconcile(final SchedulerDriver driver);

    @Subscribe
    void update(final Protos.TaskStatus status);

    boolean isReconciled();
}
