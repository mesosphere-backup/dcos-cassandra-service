package com.mesosphere.dcos.cassandra.scheduler.config;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import io.dropwizard.lifecycle.Managed;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class IdentityManager implements Managed {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IdentityManager.class);
    public static final String IDENTITY = "identity";

    private volatile Identity identity;
    private StateStore stateStore;

    private String identityError(String property, String configured, String
            persisted) {
        return String.format(
                "The framework property %s has been modified " +
                        "from %s to %s. This changes is not allowed as it " +
                        "changes the identity of the framework.",
                property, persisted, configured);
    }

    private void validate(Identity configured, Identity persisted) {
        if (!configured.getName().equals(persisted.getName())) {
            String error = identityError(
                    "name",
                    configured.getName(),
                    persisted.getName());
            LOGGER.error(error);
            throw new IllegalStateException(error);
        } else if (!configured.getPrincipal().equals(
                persisted.getPrincipal())) {
            String error = identityError(
                    "principal",
                    configured.getPrincipal(),
                    persisted.getPrincipal());
            LOGGER.error(error);

            throw new IllegalStateException(error);
        } else if (!configured.getRole().equals(persisted.getRole())) {
            String error = identityError(
                    "role",
                    configured.getRole(),
                    persisted.getRole());
            LOGGER.error(error);

            throw new IllegalStateException(error);
        } else if (!configured.getCluster().equals(persisted.getCluster())) {
            String error = identityError(
                    "cluster",
                    configured.getRole(),
                    persisted.getRole());
            LOGGER.error(error);

            throw new IllegalStateException(error);
        }
    }

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

    public boolean isRegistered() {
        return !identity.getId().isEmpty();
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
            validate(this.identity, persisted);

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
