package com.mesosphere.dcos.cassandra.scheduler.backup;

import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentMap;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupStateStore implements Managed {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupStateStore.class);

    @Inject
    public BackupStateStore() {

    }

    public void storeBackupContext(BackupContext backupContext) {
    }

    public BackupContext loadBackupContext() {
        return null;
    }

    public void store(String taskId, Protos.Offer offer) {
        LOGGER.info("Storing taskId: {} and offerId: {}", taskId, offer.getId().getValue());
        // TODO: Implementation
    }

    @Subscribe
    public void statusUpdate(Protos.TaskStatus status) {
        LOGGER.info(
                "Received status update for taskId={} state={} source={} reason={} message='{}'",
                status.getTaskId().getValue(),
                status.getState().toString(),
                status.getSource().name(),
                status.getReason().name(),
                status.getMessage());
        // TODO: Implementation
    }

    @Override
    public void start() throws Exception {
        LOGGER.debug("Starting BackupStateStore");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.debug("Stopping BackupStateStore");
    }
}
