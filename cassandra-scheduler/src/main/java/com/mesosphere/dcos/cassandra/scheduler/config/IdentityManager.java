package com.mesosphere.dcos.cassandra.scheduler.config;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentReference;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@Singleton
public class IdentityManager implements Managed {


    private static final Logger LOGGER =
            LoggerFactory.getLogger(IdentityManager.class);

    private final PersistentReference<Identity> reference;
    private volatile Identity identity;

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
            final PersistenceFactory persistence,
            final Serializer<Identity> serializer) {

        return new IdentityManager(configured,
                persistence,
                serializer);

    }

    @Inject
    public IdentityManager(
            final @Named("ConfiguredIdentity") Identity configured,
            final PersistenceFactory persistence,
            final Serializer<Identity> serializer) {
        this.identity = configured;
        this.reference = persistence.createReference("identity", serializer);


    }

    public Identity get() {

        return identity;
    }

    public boolean isRegistered() {
        return !identity.getId().isEmpty();
    }

    public synchronized void register(String id) throws PersistenceException {
        this.reference.store(identity.register(id));
        this.identity = identity.register(id);

    }

    @Override
    public void start() throws Exception {

        LOGGER.info("IdentityManager starting configured identity = {}",
                identity);

        Optional<Identity> persistedOption = reference.load();

        if (persistedOption.isPresent()) {

            Identity persisted = persistedOption.get();
            LOGGER.info("Retrieved persisted identity = {}", persisted);
            validate(identity, persisted);

            if (!persisted.getId().isEmpty()) {
                identity = identity.register(persisted.getId());
            }
        }

        LOGGER.info("Persisting identity = {}", identity);
        reference.store(identity);

    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping IdentityManager");
    }
}
