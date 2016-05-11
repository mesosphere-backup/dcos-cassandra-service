package com.mesosphere.dcos.cassandra.scheduler.persistence;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.scheduler.config.CuratorFrameworkConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.Identity;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.KeeperException;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

@Singleton
public class ZooKeeperPersistence implements PersistenceFactory {

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

        curator.close();
    }

    private static class ZooKeeperReference<T> implements
            PersistentReference<T> {

        private final CuratorFramework curator;
        private final String path;
        private final Serializer<T> serializer;

        private ZooKeeperReference(CuratorFramework curator, String path,
                                   Serializer<T> serializer) {
            this.path = path;
            this.curator = curator;
            this.serializer = serializer;
        }

        @Override
        public Optional<T> load()
                throws PersistenceException {
            try {
                byte[] bytes = curator.getData().forPath(path);
                if (bytes == null || bytes.length <= 0) {

                    return Optional.empty();
                } else {

                    return Optional.of(serializer.deserialize(bytes));
                }

            } catch (KeeperException.NoNodeException ex) {
                return Optional.empty();
            } catch (SerializationException ex) {
                throw new PersistenceException("Serialization failure", ex);
            } catch (Exception ex) {
                throw new PersistenceException("Error reading persistent " +
                        "object - path = " + path, ex);
            }
        }

        @Override
        public void store(T value)
                throws PersistenceException {

            byte[] bytes;

            try {
                bytes = serializer.serialize(value);
            } catch (SerializationException ex) {
                throw new PersistenceException("Serialization failure", ex);
            }

            try {
                curator.setData().forPath(path, bytes);
            } catch (KeeperException.NoNodeException kex) {

                try {
                    curator.create()
                            .creatingParentsIfNeeded()
                            .forPath(path, bytes);

                } catch (Exception ex) {
                    throw new PersistenceException("Error creating value  " +
                            "- path = " + path, ex);
                }
            } catch (Exception ex) {
                throw new PersistenceException("Error setting value  " +
                        "- path = " + path, ex);
            }

        }

        @Override
        public T putIfAbsent(T value) throws PersistenceException {

            byte[] bytes;

            try {
                bytes = serializer.serialize(value);

            } catch (SerializationException ex) {
                throw new PersistenceException("Serialization failure", ex);
            }


            try {
                curator.create().creatingParentsIfNeeded()
                        .forPath(path, bytes);
                return value;

            } catch (KeeperException.NodeExistsException nex) {

                try {
                    return serializer.deserialize(
                            curator.getData().forPath(path));

                } catch (Exception e) {
                    throw new PersistenceException("Error retrieving existing" +
                            " value for path - path = " + path);
                }

            } catch (Exception ex) {
                throw new PersistenceException("Error creating value" +
                        " - path = " + path, ex);
            }
        }

        @Override
        public void delete() throws PersistenceException {
            try {
                curator.delete().forPath(path);
            } catch (Exception e) {
                throw new PersistenceException("Error deleting value" +
                        " - path = " + path, e);
            }
        }
    }

    private static class ZooKeeperMap<T> implements PersistentMap<T> {

        private final CuratorFramework curator;
        private final String path;
        private final Serializer<T> serializer;

        private String path(String key) {
            return path + "/" + key;
        }

        private ZooKeeperMap(CuratorFramework curator, String path,
                             Serializer<T> serializer) {

            this.curator = curator;
            this.path = path;
            this.serializer = serializer;
        }

        @Override
        public Set<String> keySet() throws PersistenceException {

            try {
                return ImmutableSet.copyOf(curator.getChildren().forPath(path));
            } catch (KeeperException.NoNodeException ke) {
                return Collections.emptySet();
            } catch (Exception ex) {
                throw new PersistenceException("Error getting keys", ex);
            }
        }

        @Override
        public void put(String key, T value) throws PersistenceException {

            byte[] bytes;

            try {
                bytes = serializer.serialize(value);
            } catch (SerializationException ex) {
                throw new PersistenceException("Serialization failure", ex);
            }

            try {
                curator.setData().forPath(path(key), bytes);
            } catch (KeeperException.NoNodeException ke) {

                try {
                    curator.create().creatingParentsIfNeeded()
                            .forPath(path(key), bytes);
                } catch (Exception ex) {
                    throw new PersistenceException("Error creating value" +
                            " - path = " + path(key), ex);
                }
            } catch (Exception ex) {
                throw new PersistenceException("Error setting value" +
                        " - path = " + path(key), ex);
            }

        }


        @Override
        public Optional<T> get(String key) throws PersistenceException {

            byte[] bytes;

            try {
                bytes = curator.getData().forPath(path(key));
            } catch (KeeperException.NoNodeException ke) {
                return Optional.empty();
            } catch (Exception ex) {
                throw new PersistenceException("Error retrieving value" +
                        " - path  = " + path(key), ex);
            }

            try {
                return Optional.of(serializer.deserialize(bytes));
            } catch (SerializationException ex) {

                throw new PersistenceException("Serialization error", ex);
            }
        }

        @Override
        public void remove(String key) throws PersistenceException {

            try {
                curator.delete().forPath(path(key));
            } catch (KeeperException.NoNodeException nex) {

            } catch (Exception ex) {
                throw new PersistenceException("Error deleting value - path =" +
                        path(key), ex);
            }

        }
    }

    public static PersistenceFactory create(Identity identity,
                                            CuratorFrameworkConfig config) {

        return new ZooKeeperPersistence(identity, config);
    }

    private final CuratorFramework curator;

    private final String path;

    @Inject
    public ZooKeeperPersistence(Identity identity,
                                CuratorFrameworkConfig config) {

        this.path = "/cassandra/" + identity.getName();

        this.curator = CuratorFrameworkFactory.newClient(
                config.getServers(),
                (int) config.getSessionTimeoutMs(),
                (int) config.getConnectionTimeoutMs(),
                (config.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                config.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) config.getBackoffMs()) :
                        new RetryForever((int) config.getBackoffMs()));

        curator.start();

        connect();

    }

    private void connect() {

        while (true) {
            try {
                curator.blockUntilConnected();
                return;
            } catch (InterruptedException e) {

            }
        }
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    @Override
    public <T> PersistentReference<T> createReference(String name,
                                                      Serializer<T> serializer) {
        return new ZooKeeperReference<>(curator, path + "/" + name, serializer);
    }

    @Override
    public <T> PersistentMap<T> createMap(String name,
                                          Serializer<T> serializer) {
        return new ZooKeeperMap<T>(curator, path + "/" + name, serializer);
    }
}
