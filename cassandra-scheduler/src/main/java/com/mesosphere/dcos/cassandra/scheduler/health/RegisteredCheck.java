package com.mesosphere.dcos.cassandra.scheduler.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.config.IdentityManager;


public class RegisteredCheck extends HealthCheck {
    public static final String NAME = "registered";
    private final IdentityManager manager;

    @Inject
    public RegisteredCheck(final IdentityManager manager) {
        this.manager = manager;
    }

    protected Result check() throws Exception {
        String id = manager.get().getId();
        if (!id.isEmpty()) {
            return Result.healthy("Framework registered with id = " + id);
        }
        return Result.unhealthy("Framework is not yet registered");
    }
}
