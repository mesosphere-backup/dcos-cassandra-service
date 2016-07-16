package com.mesosphere.dcos.cassandra.scheduler.config;

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
    public static final String IDENTITY = "identity";

    private volatile Identity identity;
    private StateStore stateStore;

    public static IdentityManager create(
            final Identity configured,
            final StateStore stateStore) {
        return new IdentityManager(configured,
                stateStore);
    }

    @Inject
    public IdentityManager(
            final @Named("ConfiguredIdentity") Identity configured,
            final StateStore stateStore) {
        this.identity = configured;
        this.stateStore = stateStore;
    }

    public Identity get() {
        return identity;
    }

    public synchronized void register(String id) throws SerializationException {
        final Identity registeredIdentity = identity.register(id);
        this.stateStore.storeProperty(IDENTITY,  Identity.JSON_SERIALIZER.serialize(registeredIdentity));
        this.identity = identity.register(id);
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("IdentityManager starting configured identity = {}",
                identity);

        try {
            final byte[] bytesOfIdentity = stateStore.fetchProperty(IDENTITY);
            final Identity persisted = Identity.JSON_SERIALIZER.deserialize(bytesOfIdentity);

            LOGGER.info("Retrieved persisted identity = {}", persisted);

            if (!persisted.getId().isEmpty()) {
                this.identity = this.identity.register(persisted.getId());
            }
        } catch (StateStoreException e) {
            LOGGER.error("Error occured while retrieving persisted identity: ", e);
        }

        LOGGER.info("Persisting identity = {}", this.identity);
        stateStore.storeProperty(IDENTITY, Identity.JSON_SERIALIZER.serialize(this.identity));
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping IdentityManager");
    }
}
