package com.mesosphere.dcos.cassandra.scheduler.persistence;

import com.mesosphere.dcos.cassandra.common.serialization.Serializer;

import java.util.Optional;

public interface PersistentReference<T> {

    Optional<T> load() throws PersistenceException;

    void store(T value) throws PersistenceException;

    T putIfAbsent(T value) throws PersistenceException;
}
