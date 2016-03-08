package com.mesosphere.dcos.cassandra.scheduler.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.apache.mesos.reconciliation.Reconciler;

public class ReconciledCheck extends HealthCheck {
    public static final String NAME = "reconciled";
    private final Reconciler reconciler;

    @Inject
    public ReconciledCheck(final Reconciler reconciler) {
        this.reconciler = reconciler;
    }

    protected HealthCheck.Result check() throws Exception {

        if (reconciler.isReconciled()) {
            return HealthCheck.Result.healthy("Framework reconciled");
        }
        return HealthCheck.Result.unhealthy("Reconciliation in progress");
    }
}