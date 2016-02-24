package com.mesosphere.dcos.cassandra.scheduler.tasks;

import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CassandraReconciler implements Reconciler {

    private static final Logger LOGGER = LoggerFactory.getLogger
            (CassandraReconciler.class);

    private static final int MULTIPLIER = 2;
    private static final long BASE_BACKOFF_MS = 4000;
    private static final long MAX_BACKOFF_MS = 30000;

    private final AtomicBoolean reconciled = new AtomicBoolean(false);
    private final Map<String, Protos.TaskStatus> unreconciled = new HashMap<>();
    private long lastRequestTime = 0;
    private long backOff = BASE_BACKOFF_MS;
    private final CassandraTasks tasks;

    @Inject
    public CassandraReconciler(final CassandraTasks tasks) {
        this.tasks = tasks;
    }

    @Override
    public void start() {

        synchronized (unreconciled) {
            tasks.get().entrySet().stream().forEach(entry -> {
                unreconciled.put(entry.getKey(),
                        entry.getValue().getStatus().toProto());
            });
            reconciled.set(unreconciled.isEmpty());
        }
    }

    @Override
    public void reconcile(final SchedulerDriver driver) {

        if (!reconciled.get()) {
            synchronized (unreconciled) {
                final long now = System.currentTimeMillis();
                if (unreconciled.isEmpty()) {
                    LOGGER.info("Reconciled all known tasks performing final " +
                            "reconciliation request");
                    driver.reconcileTasks(Collections.emptyList());
                    lastRequestTime = 0;
                    backOff = BASE_BACKOFF_MS;
                    reconciled.set(true);
                } else if (now > lastRequestTime + backOff) {
                    LOGGER.info("Requesting reconciliation : time = {}", now);
                    lastRequestTime = now;
                    backOff = Math.min(backOff * MULTIPLIER, MAX_BACKOFF_MS);
                    driver.reconcileTasks(unreconciled.values());
                    LOGGER.info("Next reconciliation = {}", now + backOff);
                } else {
                    LOGGER.info("Unreconciled : remaining tasks = {}, " +
                                    "next reconcile time = {}",
                            unreconciled.size(),
                            lastRequestTime + backOff);
                }
            }
        }
    }

    @Override
    @Subscribe
    public void update(final Protos.TaskStatus status) {

        synchronized (unreconciled) {
            LOGGER.info("Reconciled task id = {}",
                    status.getTaskId().getValue());
            unreconciled.remove(status.getTaskId().getValue());
        }
    }

    @Override
    public boolean isReconciled() {
        return reconciled.get();
    }
}
