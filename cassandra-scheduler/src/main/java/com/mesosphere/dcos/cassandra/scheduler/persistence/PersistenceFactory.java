package com.mesosphere.dcos.cassandra.scheduler.persistence;


import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import io.dropwizard.lifecycle.Managed;

public interface PersistenceFactory extends Managed {

    <T> PersistentReference<T> createReference(String name,
                                               Serializer<T> serializer);

    <T> PersistentMap<T> createMap(String name,
                                   Serializer<T> serializer);
}
