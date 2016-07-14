package com.mesosphere.dcos.cassandra.scheduler.config;

import org.apache.mesos.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DefaultConfigurationManager {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DefaultConfigurationManager.class);

    private final ConfigStore<Configuration> configStore;

    private final Class configClass;

    private List<ConfigValidationError> validationErrors;

    public DefaultConfigurationManager(
            Class configClass,
            String storageRoot,
            String connectionHost,
            Configuration newConfiguration,
            ConfigValidator configValidator) throws ConfigStoreException {
        this.configClass = configClass;
        configStore = new CuratorConfigStore<>(storageRoot, connectionHost);
        Configuration oldConfig = null;
        try {
            oldConfig = getTargetConfig();
        } catch (ConfigStoreException e) {
            LOGGER.error("Failed to read existing target config from config store.", e);
        }
        validationErrors = configValidator.validate(oldConfig, newConfiguration);
        LOGGER.error("Validation errors: {}", validationErrors);

        if (validationErrors.isEmpty()) {
            if (!Objects.equals(newConfiguration, oldConfig)) {
                final UUID uuid = store(newConfiguration);
                LOGGER.info("Stored new configuration with UUID: " + uuid);
                setTargetName(uuid);
                LOGGER.info("Set new configuration target as UUID: " + uuid);
                // Sync

                // Cleanup
            } else {
                LOGGER.info("No config change detected.");
            }
        }
    }

    public Configuration fetch(UUID version) throws ConfigStoreException {
        try {
            final ConfigurationFactory yamlConfigurationFactory =
                    new YAMLConfigurationFactory(configClass);
            return configStore.fetch(version, yamlConfigurationFactory);
        } catch (ConfigStoreException e) {
            LOGGER.error("Unable to fetch version: " + version, e);
            throw new ConfigStoreException(e);
        }
    }

    /**
     * Returns whether a current target configuration exists.
     */
    public boolean hasTarget() {
        try {
            return configStore.getTargetConfig() != null;
        } catch (Exception e) {
            LOGGER.error("Failed to determine target config", e);
            return false;
        }
    }

    /**
     * Returns the name of the current target configuration.
     */
    public UUID getTargetName() throws ConfigStoreException {
        try {
            return configStore.getTargetConfig();
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve target config name", e);
            throw e;
        }
    }

    /**
     * Returns the current target configuration.
     *
     * @throws ConfigStoreException if the underlying storage failed to read
     */
    public Configuration getTargetConfig() throws ConfigStoreException {
        return fetch(getTargetName());
    }

    /**
     * Returns a list of all available configuration names.
     *
     * @throws ConfigStoreException if the underlying storage failed to read
     */
    public Collection<UUID> getConfigNames() throws ConfigStoreException {
        return configStore.list();
    }

    /**
     * Stores the provided configuration against the provided version label.
     *
     * @throws ConfigStoreException if the underlying storage failed to write
     */
    public UUID store(Configuration configuration) throws ConfigStoreException {
        try {
            return configStore.store(configuration);
        } catch (Exception e) {
            String msg = "Failure to store configurations.";
            LOGGER.error(msg, e);
            throw new ConfigStoreException(msg, e);
        }
    }

    /**
     * Sets the name of the target configuration to be used in the future.
     */
    public void setTargetName(UUID targetConfigName) throws ConfigStoreException {
        try {
            configStore.setTargetConfig(targetConfigName);
        } catch (Exception ex) {
            String msg = "Failed to set target config with exception";
            LOGGER.error(msg, ex);
            throw new ConfigStoreException(msg, ex);
        }
    }

    public List<ConfigValidationError> getErrors() {
        return validationErrors;
    }
}
