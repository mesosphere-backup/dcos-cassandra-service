package com.mesosphere.dcos.cassandra.scheduler.config;

import org.apache.mesos.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConfigValidator {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConfigValidator.class);

    public ConfigValidation serversValidation = (oldConfig, newConfig) -> {
        List<ConfigValidationError> errors = new LinkedList<>();
        if (oldConfig == null) {
            return errors;
        }
        CassandraSchedulerConfiguration oldConfiguration = (CassandraSchedulerConfiguration) oldConfig;
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        if (newConfiguration.getServers() < oldConfiguration.getServers()) {
            final String errorMessage = String.format("The number of configured " +
                            "nodes (%d) is less than the previous " +
                            "number of configured nodes (%d).",
                    newConfiguration.getServers(),
                    oldConfiguration.getServers());
            final ConfigValidationError error = new ConfigValidationError("servers", errorMessage);
            errors.add(error);
        }
        return errors;
    };

    public ConfigValidation seedValidation = (oldConfig, newConfig) -> {
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        List<ConfigValidationError> errors = new LinkedList<>();
        if (newConfiguration.getServers() < newConfiguration.getSeeds()) {
            String errorMessage = String.format("The number of configured " +
                            "seeds (%d) is greater than the current number " +
                            "of configured nodes (%d). Reduce the " +
                            "number of seeds or increase the number of nodes",
                    newConfiguration.getSeeds(),
                    newConfiguration.getServers());
            final ConfigValidationError error = new ConfigValidationError("seeds", errorMessage);
            errors.add(error);
        }
        return errors;
    };

    public ConfigValidation diskTypeValidation = (oldConfig, newConfig) -> {
        List<ConfigValidationError> errors = new LinkedList<>();
        if (oldConfig == null) {
            return errors;
        }
        CassandraSchedulerConfiguration oldConfiguration = (CassandraSchedulerConfiguration) oldConfig;
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        if (!Objects.equals(newConfiguration.getCassandraConfig().getDiskType()
                , oldConfiguration.getCassandraConfig().getDiskType())) {
            final String errorMessage = String.format("The configured disk type can " +
                            "not be changed. Old disk type is (%s). " +
                            "New disk type is (%s)",
                    oldConfiguration.getCassandraConfig().getDiskType(),
                    newConfiguration.getCassandraConfig().getDiskType());
            final ConfigValidationError error = new ConfigValidationError("diskType", errorMessage);
            errors.add(error);
        }
        return errors;
    };

    public ConfigValidation diskSizeValidation = (oldConfig, newConfig) -> {
        List<ConfigValidationError> errors = new LinkedList<>();
        if (oldConfig == null) {
            return errors;
        }
        CassandraSchedulerConfiguration oldConfiguration = (CassandraSchedulerConfiguration) oldConfig;
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        if (!Objects.equals(newConfiguration.getCassandraConfig().getDiskMb()
                , oldConfiguration.getCassandraConfig().getDiskMb())) {
            final String errorMessage = String.format("The configured volume size can" +
                            " not be changed. Old volume size is " +
                            "(%d) Mb. New volume size is (%d) Mb",
                    oldConfiguration.getCassandraConfig().getDiskMb(),
                    newConfiguration.getCassandraConfig().getDiskMb());
            final ConfigValidationError error = new ConfigValidationError("diskMb", errorMessage);
            errors.add(error);
        }
        return errors;
    };

    public Collection<ConfigValidation> validations = Arrays.asList(
            serversValidation,
            seedValidation,
            diskTypeValidation,
            diskSizeValidation);

    public List<ConfigValidationError> validate(Configuration oldConfig, Configuration newConfig) {
        List<ConfigValidationError> errors = new ArrayList<>();
        for (ConfigValidation validation : validations) {
            errors.addAll(validation.validate(oldConfig, newConfig));
        }
        return errors;
    }
}
