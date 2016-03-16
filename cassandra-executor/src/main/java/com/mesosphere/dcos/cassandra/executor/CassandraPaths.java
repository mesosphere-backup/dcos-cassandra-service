package com.mesosphere.dcos.cassandra.executor;


import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.Location;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CassandraPaths {

    private final Path root;

    public static CassandraPaths create(final String version) {
        return new CassandraPaths(version);
    }

    public CassandraPaths(final String version) {
        root = Paths.get("apache-cassandra-" + version);
    }

    public Path conf() {
        return root.resolve("conf");
    }

    public Path bin() {
        return root.resolve("bin");
    }

    public Path lib() {

        return root.resolve("lib");
    }

    public Path tools() {
        return root.resolve("tools");
    }

    public Path cassandraConfig() {
        return conf().resolve(CassandraApplicationConfig.DEFAULT_FILE_NAME);
    }

    public Path cassandraLocation() {
        return conf().resolve(Location.DEFAULT_FILE);
    }

    public Path cassandraRun() {

        return bin().resolve("cassandra");
    }
}
