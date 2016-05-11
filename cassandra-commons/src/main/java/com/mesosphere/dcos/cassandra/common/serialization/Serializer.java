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
package com.mesosphere.dcos.cassandra.common.serialization;


/**
 * Interface for Serialization objects.
 * @param <T> The type that is serialized and deserialized by implementations.
 */
public interface Serializer<T> {

    /**
     * Creates a byte array that contains a binary representation of
     * @param value The value that will be serialized.
     * @return A byte array containing the binary representation of value.
     * @throws SerializationException If value could not be serialized.
     */
    byte[] serialize(final T value) throws SerializationException;

    /**
     * Creates an Object instance parsed from bytes.
     * @param bytes A byte array containing a serialized object.
     * @return The instance parsed from bytes.
     * @throws SerializationException If an Object could not be parsed from
     * bytes.
     */
    T deserialize(final byte[] bytes) throws SerializationException;
}
