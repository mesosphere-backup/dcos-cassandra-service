package com.mesosphere.dcos.cassandra.scheduler.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;


public class RegisteredCheck extends HealthCheck {
    public static final String NAME = "registered";
    private final StateStore stateStore;

    @Inject
    public RegisteredCheck(final StateStore stateStore) {
        this.stateStore = stateStore;
    }

    protected Result check() throws Exception {
        try {
            final Protos.FrameworkID frameworkID = stateStore.fetchFrameworkId();
            String id = frameworkID.getValue();
            if (!id.isEmpty()) {
                return Result.healthy("Framework registered with id = " + id);
            }
            return Result.unhealthy("Framework is not yet registered");
        } catch (StateStoreException e) {
            return Result.unhealthy("Framework is not yet registered");
        } catch (Throwable t) {
            throw t;
        }
    }
}
