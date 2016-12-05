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
package com.mesosphere.dcos.cassandra.common.util;

import java.io.IOException;

import org.apache.mesos.config.SerializationUtils;

/**
 * Contains static Json manipulation utilities.
 */
public class JsonUtils {

    private JsonUtils() {
        // do not instantiate
    }

    /**
     * Gets a JSON representation of value.
     *
     * TODO(nick): Replace with SerializationUtils.toJsonStringOrEmpty() once that's available
     */
    public static <T> String toJsonString(T value) {
        try {
            return SerializationUtils.toJsonString(value);
        } catch (IOException e) {
            return "";
        }
    }
}
