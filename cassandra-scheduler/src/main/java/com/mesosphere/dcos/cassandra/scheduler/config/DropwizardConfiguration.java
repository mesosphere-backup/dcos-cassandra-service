package com.mesosphere.dcos.cassandra.scheduler.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import java.util.Objects;

public class DropwizardConfiguration extends Configuration {
    @JsonProperty("schedulerConfiguration")
    private CassandraSchedulerConfiguration schedulerConfiguration;

    @JsonCreator
    public DropwizardConfiguration(
            @JsonProperty("schedulerConfiguration") CassandraSchedulerConfiguration schedulerConfiguration) {
        this.schedulerConfiguration = schedulerConfiguration;
    }

    public CassandraSchedulerConfiguration getSchedulerConfiguration() {
        return schedulerConfiguration;
    }

    @JsonProperty("schedulerConfiguration")
    public void setSchedulerConfiguration(CassandraSchedulerConfiguration schedulerConfiguration) {
        this.schedulerConfiguration = schedulerConfiguration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropwizardConfiguration that = (DropwizardConfiguration) o;
        return Objects.equals(schedulerConfiguration, that.schedulerConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schedulerConfiguration);
    }

    @Override
    public String toString() {
        return "DropwizardConfiguration{" +
                "schedulerConfiguration=" + schedulerConfiguration +
                '}';
    }
}
