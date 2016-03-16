package com.mesosphere.dcos.cassandra.scheduler.persistence;

import java.util.Optional;

public interface PersistentReference<T> {
    Optional<T> load() throws PersistenceException;

    void store(T value) throws PersistenceException;

    T putIfAbsent(T value) throws PersistenceException;

    void delete() throws PersistenceException;
}
