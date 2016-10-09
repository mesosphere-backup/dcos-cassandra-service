package com.mesosphere.dcos.cassandra.common.config;

import org.apache.mesos.config.Configuration;

import java.util.*;

public class ConfigValidator {

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
            final String errorMessage = String.format("The configured disk size can" +
                            " not be changed. Persisted disk size is " +
                            "(%d) Mb. Configured disk size is (%d) Mb",
                    oldConfiguration.getCassandraConfig().getDiskMb(),
                    newConfiguration.getCassandraConfig().getDiskMb());
            final ConfigValidationError error = new ConfigValidationError("diskMb", errorMessage);
            errors.add(error);
        }
        return errors;
    };

    public ConfigValidation frameworkNameValidation = ((oldConfig, newConfig) -> {
        List<ConfigValidationError> errors = new LinkedList<>();
        if (oldConfig == null) {
            return errors;
        }
        CassandraSchedulerConfiguration oldConfiguration = (CassandraSchedulerConfiguration) oldConfig;
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        final String newName = newConfiguration.getServiceConfig().getName();
        final String oldName = oldConfiguration.getServiceConfig().getName();
        if (!Objects.equals(newName, oldName)) {
            final String errorMessage = String.format("The configured name can not be changed. Old name is (%s). " +
                            "New name is (%s)", oldName, newName);
            final ConfigValidationError error = new ConfigValidationError("name", errorMessage);
            errors.add(error);
        }
        return errors;
    });

    public ConfigValidation principalValidation = ((oldConfig, newConfig) -> {
        List<ConfigValidationError> errors = new LinkedList<>();
        if (oldConfig == null) {
            return errors;
        }
        CassandraSchedulerConfiguration oldConfiguration = (CassandraSchedulerConfiguration) oldConfig;
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        final String newPrincipal = newConfiguration.getServiceConfig().getPrincipal();
        final String oldPrincipal = oldConfiguration.getServiceConfig().getPrincipal();
        if (!Objects.equals(newPrincipal, oldPrincipal)) {
            final String errorMessage = String.format("The configured principal can not be changed. Old principal is (%s). " +
                    "New principal is (%s)", oldPrincipal, newPrincipal);
            final ConfigValidationError error = new ConfigValidationError("principal", errorMessage);
            errors.add(error);
        }
        return errors;
    });

    public ConfigValidation roleValidation = ((oldConfig, newConfig) -> {
        List<ConfigValidationError> errors = new LinkedList<>();
        if (oldConfig == null) {
            return errors;
        }
        CassandraSchedulerConfiguration oldConfiguration = (CassandraSchedulerConfiguration) oldConfig;
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        final String newRole = newConfiguration.getServiceConfig().getRole();
        final String oldRole = oldConfiguration.getServiceConfig().getRole();
        if (!Objects.equals(newRole, oldRole)) {
            final String errorMessage = String.format("The configured role can not be changed. Old role is (%s). " +
                    "New role is (%s)", oldRole, newRole);
            final ConfigValidationError error = new ConfigValidationError("role", errorMessage);
            errors.add(error);
        }
        return errors;
    });

    public ConfigValidation clusterValidation = ((oldConfig, newConfig) -> {
        List<ConfigValidationError> errors = new LinkedList<>();
        if (oldConfig == null) {
            return errors;
        }
        CassandraSchedulerConfiguration oldConfiguration = (CassandraSchedulerConfiguration) oldConfig;
        CassandraSchedulerConfiguration newConfiguration = (CassandraSchedulerConfiguration) newConfig;
        final String newCluster = newConfiguration.getServiceConfig().getCluster();
        final String oldCluster = oldConfiguration.getServiceConfig().getCluster();
        if (!Objects.equals(newCluster, oldCluster)) {
            final String errorMessage = String.format("The configured cluster can not be changed. Old cluster is (%s). " +
                    "New cluster is (%s)", oldCluster, newCluster);
            final ConfigValidationError error = new ConfigValidationError("cluster", errorMessage);
            errors.add(error);
        }
        return errors;
    });

    public Collection<ConfigValidation> validations = Arrays.asList(
            serversValidation,
            seedValidation,
            diskTypeValidation,
            diskSizeValidation,
            frameworkNameValidation,
            principalValidation,
            roleValidation,
            clusterValidation);

    public List<ConfigValidationError> validate(Configuration oldConfig, Configuration newConfig) {
        List<ConfigValidationError> errors = new ArrayList<>();
        for (ConfigValidation validation : validations) {
            errors.addAll(validation.validate(oldConfig, newConfig));
        }
        return errors;
    }
}
