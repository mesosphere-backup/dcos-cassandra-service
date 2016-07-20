package com.mesosphere.dcos.cassandra.scheduler.config;

import org.apache.mesos.config.Configuration;

import java.util.List;

public interface ConfigValidation {
    List<ConfigValidationError> validate(Configuration oldConfig, Configuration newConfig);
}
