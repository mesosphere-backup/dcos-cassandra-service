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
package com.mesosphere.dcos.cassandra.common.tasks;

/**
 * Enumeration of the modes of a Cassandra node.
 */
public enum CassandraMode {
    /**
     * The node is starting but has not yet attempted to join the cluster.
     */
    STARTING,
    /**
     * The node has joined the cluster and is not leaving or draining.
     */
    NORMAL,
    /**
     * The node is attempting to join the cluster.
     */
    JOINING,
    /**
     * The node is leaving the cluster.
     */
    LEAVING,
    /**
     * The node has been decommissioned and can be removed from the ring.
     */
    DECOMMISSIONED,
    /**
     * The nodes tokens are being moved.
     */
    MOVING,
    /**
     * The node is being drained of connections.
     */
    DRAINING,
    /**
     * The node has been drained of connections.
     */
    DRAINED
}
