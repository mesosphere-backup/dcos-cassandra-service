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
 * Serializes a Boolean value to and from a String.
 */
public class BooleanStringSerializer implements Serializer<Boolean> {

    private static final BooleanStringSerializer instance = new
            BooleanStringSerializer();

    private BooleanStringSerializer() {
    }

    /**
     * Gets the singleton instance.
     * @return The singleton instance for BooleanStringSerializer.
     */
    public static BooleanStringSerializer get() {
        return instance;
    }

    @Override
    public byte[] serialize(Boolean value) throws SerializationException {
        return value.toString().getBytes();
    }

    @Override
    public Boolean deserialize(byte[] bytes)
            throws SerializationException {
        return Boolean.parseBoolean(new String(bytes));
    }
}
