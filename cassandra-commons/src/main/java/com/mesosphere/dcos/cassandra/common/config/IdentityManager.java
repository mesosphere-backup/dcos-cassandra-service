package com.mesosphere.dcos.cassandra.common.config;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class IdentityManager implements Managed {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IdentityManager.class);
    public static final String IDENTITY = "serviceConfig";

    private volatile ServiceConfig serviceConfig;
    private StateStore stateStore;

    public static IdentityManager create(
            final ServiceConfig configured,
            final StateStore stateStore) {
        return new IdentityManager(configured,
                stateStore);
    }

    @Inject
    public IdentityManager(
            final @Named("ConfiguredIdentity") ServiceConfig configured,
            final StateStore stateStore) {
        this.serviceConfig = configured;
        this.stateStore = stateStore;
    }

    public ServiceConfig get() {
        return serviceConfig;
    }

    public synchronized void register(String id) throws SerializationException {
        final ServiceConfig registeredServiceConfig = serviceConfig.register(id);
        this.stateStore.storeProperty(IDENTITY,  ServiceConfig.JSON_SERIALIZER.serialize(registeredServiceConfig));
        this.serviceConfig = serviceConfig.register(id);
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("IdentityManager starting configured serviceConfig = {}",
          serviceConfig);

        try {
            final byte[] bytesOfIdentity = stateStore.fetchProperty(IDENTITY);
            final ServiceConfig persisted = ServiceConfig.JSON_SERIALIZER.deserialize(bytesOfIdentity);

            LOGGER.info("Retrieved persisted serviceConfig = {}", persisted);

            if (!persisted.getId().isEmpty()) {
                this.serviceConfig = this.serviceConfig.register(persisted.getId());
            }
        } catch (StateStoreException e) {
            LOGGER.error("Error occured while retrieving persisted serviceConfig: ", e);
        }

        LOGGER.info("Persisting serviceConfig = {}", this.serviceConfig);
        stateStore.storeProperty(IDENTITY, ServiceConfig.JSON_SERIALIZER.serialize(this.serviceConfig));
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping IdentityManager");
    }
}
