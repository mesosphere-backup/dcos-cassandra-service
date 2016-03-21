package com.mesosphere.dcos.cassandra.scheduler.persistence;

import java.util.Optional;
import java.util.Set;

public interface PersistentMap<T> {

    Set<String> keySet() throws PersistenceException;

    void put(String string, T value) throws PersistenceException;

    Optional<T> get(String key) throws PersistenceException;

    void remove(String key) throws PersistenceException;


}
