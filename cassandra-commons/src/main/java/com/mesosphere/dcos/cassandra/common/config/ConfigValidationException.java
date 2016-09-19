package com.mesosphere.dcos.cassandra.common.config;

import java.util.Collection;

public class ConfigValidationException extends Exception {
    private final Collection<ConfigValidationError> validationErrors;

    public ConfigValidationException(Collection<ConfigValidationError> validationErrors) {
        super(String.format("%d validation errors: %s",
                validationErrors.size(), validationErrors.toString()));
        this.validationErrors = validationErrors;
    }

    public Collection<ConfigValidationError> getValidationErrors() {
        return validationErrors;
    }
}
