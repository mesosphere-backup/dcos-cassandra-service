package com.mesosphere.dcos.cassandra.common.serialization;


public interface Serializer<T> {

    byte [] serialize(final T value) throws SerializationException;

    T deserialize(final byte[] bytes) throws SerializationException;
}
