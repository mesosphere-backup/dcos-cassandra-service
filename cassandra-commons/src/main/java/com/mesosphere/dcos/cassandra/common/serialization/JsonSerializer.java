package com.mesosphere.dcos.cassandra.common.serialization;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

public class JsonSerializer<T> implements Serializer<T> {

    private final Class<T> clazz;

    public static <T> JsonSerializer<T> create(Class<T> clazz) {
        return new JsonSerializer<T>(clazz);
    }

    private JsonSerializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(T value) throws SerializationException {
        try {
            return JsonUtils.MAPPER.writeValueAsBytes(value);
        } catch (JsonProcessingException ex) {
            throw new SerializationException(
                    String.format("Error serializing %s to JSON", clazz.getName()), ex);
        }
    }

    @Override
    public T deserialize(byte[] bytes) throws SerializationException {
        try {
            return JsonUtils.MAPPER.readValue(bytes, clazz);
        } catch (IOException ex) {
            throw new SerializationException(
                    String.format("Error deserializing %s from JSON", clazz.getName()), ex);
        }
    }
}
