package com.mesosphere.dcos.cassandra.executor;
/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.Location;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * CassandraPaths implements the logic to access directories and files in a
 * Cassandra distribution.
 */
public class CassandraPaths {

    private final Path root;

    /**
     * Creates a new CassandraPaths for the indicated version.
     *
     * @param version The version of Cassandra (e.g. 2.2.6)
     * @return A new CassandraPaths for version.
     */
    public static CassandraPaths create(final String version) {
        return new CassandraPaths(version);
    }

    /**
     * Constructs a new CassandraPaths for the indicated version.
     *
     * @param version The version of Cassandra (e.g. 2.2.6)
     */
    public CassandraPaths(final String version) {
        root = Paths.get("apache-cassandra-" + version);
    }

    /**
     * Gets the configuration directory
     *
     * @return The Cassandra configuration directory.
     */
    public Path conf() {
        return root.resolve("conf");
    }

    /**
     * Gets the bin directory.
     *
     * @return The Cassandra bin directory.
     */
    public Path bin() {
        return root.resolve("bin");
    }

    /**
     * Gets the lib directory.
     *
     * @return The Cassandra lib directory.
     */
    public Path lib() {

        return root.resolve("lib");
    }

    /**
     * Gets the tools directory.
     *
     * @return The Cassandra tools directory.
     */
    public Path tools() {
        return root.resolve("tools");
    }

    /**
     * Gets the Cassandra configuration file.
     *
     * @return The cassandra.yaml configuration file.
     */
    public Path cassandraConfig() {
        return conf().resolve(CassandraApplicationConfig.DEFAULT_FILE_NAME);
    }

    /**
     * Gets the Cassandra snitch file.
     *
     * @return The gossip property files snitch file.
     */
    public Path cassandraLocation() {
        return conf().resolve(Location.DEFAULT_FILE);
    }

    /**
     * Gets the Cassandra run command.
     *
     * @return The Cassandra run command.
     */
    public Path cassandraRun() {

        return bin().resolve("cassandra");
    }
}
