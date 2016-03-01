package com.mesosphere.dcos.cassandra.scheduler.persistence;

import com.google.common.collect.ImmutableSet;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryPersistence implements PersistenceFactory {

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    private static class InMemoryReference<T>
            implements PersistentReference<T> {

        private final AtomicReference<Optional<byte[]>> reference =
                new AtomicReference<>(Optional.empty());

        private final Serializer<T> serializer;

        public static <U> InMemoryReference<U> create(
                Serializer<U> serializer) {
            return new InMemoryReference<>(serializer);
        }

        public InMemoryReference(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public Optional<T> load()
                throws PersistenceException {

            Optional<byte[]> bytesOption = reference.get();

            if (!bytesOption.isPresent()) {
                return Optional.empty();
            } else {
                try {
                    return Optional.of(
                            serializer.deserialize(bytesOption.get()));
                } catch (SerializationException ex) {

                    throw new PersistenceException("Serialization failure", ex);
                }
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
            reference.set(Optional.of(bytes));

        }

        @Override
        public T putIfAbsent(T value) throws PersistenceException {

            byte[] bytes;
            try {
                bytes = serializer.serialize(value);
            } catch (SerializationException ex) {

                throw new PersistenceException("Serialization failure", ex);
            }
            if (reference.compareAndSet(Optional.empty(),
                    Optional.of(bytes))) {

                return value;
            } else {

                try {
                    return serializer.deserialize(reference.get().get());
                } catch (SerializationException ex) {
                    throw new PersistenceException("Serialization failure", ex);
                }
            }
        }

        @Override
        public void delete() throws PersistenceException {
            reference.set(Optional.empty());
        }
    }

    private static class InMemoryMap<T> implements PersistentMap<T> {

        public static <U> InMemoryMap<U> create(Serializer<U> serializer) {
            return new InMemoryMap<>(serializer);
        }

        private final ConcurrentHashMap<String, byte[]> map =
                new ConcurrentHashMap<>();
        private final Serializer<T> serializer;


        private InMemoryMap(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public Set<String> keySet() throws PersistenceException {
            return ImmutableSet.copyOf(map.keySet());
        }

        @Override
        public void put(String string, T value)
                throws PersistenceException {
            try {
                map.put(string, serializer.serialize(value));
            } catch (SerializationException ex) {
                throw new PersistenceException("Serialization failure", ex);
            }

        }

        @Override
        public Optional<T> get(String key)
                throws PersistenceException {

            byte[] bytes = map.get(key);
            if (bytes == null) {
                return Optional.empty();
            } else {

                try {
                    return Optional.of(serializer.deserialize(bytes));
                } catch (SerializationException ex) {
                    throw new PersistenceException("Serialization failure", ex);
                }
            }
        }

        @Override
        public void remove(String key) throws PersistenceException {

            map.remove(key);
        }
    }

    public static PersistenceFactory create() {
        return new InMemoryPersistence();
    }

    @Override
    public <T> PersistentReference<T> createReference(String name,
                                                      Serializer<T> serializer) {
        return new InMemoryReference(serializer);
    }

    @Override
    public <T> PersistentMap<T> createMap(String name,
                                          Serializer<T> serializer) {
        return new InMemoryMap<>(serializer);
    }
}
