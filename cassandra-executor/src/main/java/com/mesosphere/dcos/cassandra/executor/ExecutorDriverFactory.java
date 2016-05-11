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
package com.mesosphere.dcos.cassandra.executor;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;

/**
 * The interface for the creation of ExecutorDrivers. This is necessary for
 * dependency injection.
 */
public interface ExecutorDriverFactory {

    /**
     * Gets a driver.
     * @param executor The Executor for which the driver will be returned.
     * @return The ExecutorDriver for executor
     */
    ExecutorDriver getDriver(Executor executor);
}
