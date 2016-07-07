package com.mesosphere.dcos.cassandra.executor;


import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.Location;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DsePaths {

    private final Path root;

    /**
     * Creates a new DsePaths for the indicated version.
     *
     * @param version The version of DSE (e.g. 5.0.0)
     * @return A new DSEPaths for version.
     */
    public static DsePaths create(final String version) {
        return new DsePaths(version);
    }

    /**
     * Constructs a new DsePaths for the indicated version.
     *
     * @param version The version of DSE (e.g. 5.0.0)
     */
    public DsePaths(final String version) {
        root = Paths.get("dse-" + version);
    }

    public CassandraPaths cassandra(){
        return CassandraPaths.create(
                root.resolve("resources").resolve("cassandra"));
    }

    public Path home(){
        return root;
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
     * Gets the Cassandra run command.
     *
     * @return The Cassandra run command.
     */
    public Path dseRun() {
        return bin().resolve("dse");
    }

    /**
     * CassandraPaths implements the logic to access directories and files in a
     * Cassandra distribution.
     */
    public static class CassandraPaths {

        private final Path root;

        private static CassandraPaths create(final Path root) {
            return new CassandraPaths(root);
        }


        private CassandraPaths(final Path root) {
            this.root = root;
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
         * Gets the configuration directory
         *
         * @return The Cassandra configuration directory.
         */
        public Path conf() {
            return root.resolve("conf");
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

}
