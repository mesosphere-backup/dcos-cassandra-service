package com.mesosphere.dcos.cassandra.common.config;

import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStore;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.Configuration;
import org.apache.mesos.config.ConfigurationFactory;
import org.apache.mesos.curator.CuratorConfigStore;
import org.apache.mesos.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DefaultConfigurationManager {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DefaultConfigurationManager.class);

    private final ConfigStore<Configuration> configStore;
    private final Class<?> configClass;

    private List<ConfigValidationError> validationErrors;
    private StateStore stateStore;

    public DefaultConfigurationManager(
            Class<?> configClass,
            String frameworkName,
            String connectionHost,
            Configuration newConfiguration,
            ConfigValidator configValidator,
            StateStore stateStore) throws ConfigStoreException {
        this.configClass = configClass;
        this.stateStore = stateStore;
        configStore = new CuratorConfigStore<>(frameworkName, connectionHost);
        Configuration oldConfig = null;
        try {
            UUID targetName = getTargetName();
            LOGGER.info("Current target config: {}", targetName.toString());
            oldConfig = fetch(targetName);
        } catch (ConfigStoreException e) {
            // just print the message, not the full stacktrace. then continue with newConfiguration.
            // avoid making anyone think this is an error, while still providing enough info just in
            // case it is
            LOGGER.info("Unable to retrieve target config ID from config store. " +
                "This is expected if the framework is starting for the first time: {}",
                e.getMessage());
        }
        validationErrors = configValidator.validate(oldConfig, newConfiguration);
        LOGGER.error("Validation errors: {}", validationErrors);

        if (validationErrors.isEmpty()) {
            if (!Objects.equals(newConfiguration, oldConfig)) {
                LOGGER.info("Config change detected");
                final UUID uuid = store(newConfiguration);
                LOGGER.info("Stored new configuration with UUID: " + uuid);
                setTargetName(uuid);
                LOGGER.info("Set new configuration target as UUID: " + uuid);
                syncConfigs();
                cleanConfigs();
            } else {
                LOGGER.info("No config change detected.");
            }
        }
    }

    private void cleanConfigs() throws ConfigStoreException {
        Set<UUID> activeConfigs = new HashSet<>();
        activeConfigs.add(getTargetName());
        activeConfigs.addAll(getTaskConfigs());

        LOGGER.info("Cleaning configs which are NOT in the active list: {}", activeConfigs);

        for (UUID configName : getConfigNames()) {
            if (!activeConfigs.contains(configName)) {
                try {
                    LOGGER.info("Removing config: {}", configName);
                    configStore.clear(configName);
                } catch (ConfigStoreException e) {
                    LOGGER.error("Unable to clear config: {} Reason: {}", configName, e);
                }
            }
        }
    }

    private Set<UUID> getTaskConfigs() {
        final Collection<Protos.TaskInfo> taskInfos = stateStore.fetchTasks();
        final Set<UUID> activeConfigs = new HashSet<>();
        try {
            for (Protos.TaskInfo taskInfo : taskInfos) {
                final Protos.Labels labels = taskInfo.getLabels();
                for(Protos.Label label : labels.getLabelsList()) {
                    if (label.getKey().equals(PersistentOfferRequirementProvider.CONFIG_TARGET_KEY)) {
                        activeConfigs.add(UUID.fromString(label.getValue()));
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to fetch configurations from taskInfos.", e);
        }

        return activeConfigs;
    }

    private void syncConfigs() throws ConfigStoreException {
        try {
            final UUID targetConfigName = getTargetName();
            final List<String> duplicateConfigs = getDuplicateConfigs();

            LOGGER.info("Syncing configs. Target: {} Duplicate: {}", targetConfigName.toString(), duplicateConfigs);

            final Collection<Protos.TaskInfo> taskInfos = stateStore.fetchTasks();

            for(Protos.TaskInfo taskInfo : taskInfos) {
                replaceDuplicateConfig(taskInfo, stateStore, duplicateConfigs, targetConfigName);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to sync configurations", e);
            throw new ConfigStoreException(e);
        }
    }

    private void replaceDuplicateConfig(Protos.TaskInfo taskInfo,
                                        StateStore stateStore,
                                        List<String> duplicateConfigs,
                                        UUID targetName) throws ConfigStoreException {
        try {
            final String taskConfigName = getConfigName(taskInfo);
            final String targetConfigName = targetName.toString();

            for(String duplicateConfig : duplicateConfigs) {
                if (duplicateConfig.equals(taskConfigName)) {
                    final Protos.Label configTargetKeyLabel = Protos.Label.newBuilder()
                            .setKey(PersistentOfferRequirementProvider.CONFIG_TARGET_KEY)
                            .setValue(targetConfigName)
                            .build();

                    final Protos.Labels labels = Protos.Labels.newBuilder()
                            .addLabels(configTargetKeyLabel)
                            .build();

                    final Protos.TaskInfo updatedTaskInfo = Protos.TaskInfo.newBuilder(taskInfo)
                            .setLabels(labels).build();
                    stateStore.storeTasks(Arrays.asList(updatedTaskInfo));
                    LOGGER.info("Updated task: {} from duplicate config: {} to current target: {}",
                            updatedTaskInfo.getName(), taskConfigName, targetConfigName);
                    return;
                }
            }
            LOGGER.info("Task: {} is update to date with target config: {}", taskInfo.getName(), targetConfigName);
        } catch (Exception e) {
            LOGGER.error("Failed to replace duplicate config for task: {} Reason: {}", taskInfo, e);
            throw new ConfigStoreException(e);
        }
    }

    private static String getConfigName(Protos.TaskInfo taskInfo) {
        for (Protos.Label label : taskInfo.getLabels().getLabelsList()) {
            if (label.getKey().equals("config_target")) {
                return label.getValue();
            }
        }

        return null;
    }

    private List<String> getDuplicateConfigs() throws ConfigStoreException {
        final CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration) getTargetConfig();

        final List<String> duplicateConfigs = new ArrayList<>();
        final CassandraConfig targetCassandraConfig = targetConfig.getCassandraConfig();
        final ClusterTaskConfig targetClusterTaskConfig = targetConfig.getClusterTaskConfig();
        final ExecutorConfig targetExecutorConfig = targetConfig.getExecutorConfig();

        final Collection<UUID> configNames = getConfigNames();
        for (UUID configName : configNames) {
            final CassandraSchedulerConfiguration config = (CassandraSchedulerConfiguration) fetch(configName);
            final CassandraConfig cassandraConfig = config.getCassandraConfig();
            final ClusterTaskConfig clusterTaskConfig = config.getClusterTaskConfig();
            final ExecutorConfig executorConfig = config.getExecutorConfig();

            if (cassandraConfig.equals(targetCassandraConfig) &&
                    clusterTaskConfig.equals(targetClusterTaskConfig) &&
                    executorConfig.equals(targetExecutorConfig)) {
                LOGGER.info("Duplicate config detected: {}", configName);
                duplicateConfigs.add(configName.toString());
            }
        }

        return duplicateConfigs;
    }

    private Configuration fetch(UUID version) throws ConfigStoreException {
        try {
            final ConfigurationFactory<Configuration> yamlConfigurationFactory =
                    new YAMLConfigurationFactory(configClass);
            return configStore.fetch(version, yamlConfigurationFactory);
        } catch (ConfigStoreException e) {
            LOGGER.error("Unable to fetch version: " + version, e);
            throw new ConfigStoreException(e);
        }
    }

    /**
     * Returns the name of the current target configuration.
     */
    public UUID getTargetName() throws ConfigStoreException {
        return configStore.getTargetConfig();
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
    private Collection<UUID> getConfigNames() throws ConfigStoreException {
        return configStore.list();
    }

    /**
     * Stores the provided configuration against the provided version label.
     *
     * @throws ConfigStoreException if the underlying storage failed to write
     */
    UUID store(Configuration configuration) throws ConfigStoreException {
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
    private void setTargetName(UUID targetConfigName) throws ConfigStoreException {
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
