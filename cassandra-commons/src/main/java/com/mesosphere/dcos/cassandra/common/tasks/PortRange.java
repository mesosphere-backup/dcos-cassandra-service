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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos.Value.Range;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * PortRange is used to represent a range of ports associated with a task.
 */
public final class PortRange {

    @JsonProperty("begin")
    private final int begin;
    @JsonProperty("end")
    private final int end;

    /**
     * Creates a PortRange [being,end].
     *
     * @param begin The first port in the range.
     * @param end   The last port in the range.
     * @return A PortRange [begin,end].
     */
    @JsonCreator
    public static PortRange create(
            @JsonProperty("begin") int begin,
            @JsonProperty("end") int end) {
        return new PortRange(begin, end);
    }

    /**
     * Parses a PortRange from a Protocol Buffers represenation.
     *
     * @param range The Range containing the PortRange.
     * @return The PortRange parsed from range.
     */
    public static PortRange parse(Range range) {
        return create((int) range.getBegin(), (int) range.getEnd());
    }

    /**
     * Constructs a List of PortRanges from a sequence of ports.
     *
     * @param ports The ports that will be used to construct the PortRanges.
     * @return A List of PortRanges that covers the sequence of ports such
     * that any two ports that are directly adjacent willbe in the same range.
     */
    public static List<PortRange> fromPorts(int... ports) {

        Arrays.sort(ports);
        final LinkedList<PortRange> ranges = new LinkedList<>();

        if (ports.length <= 0) {
            return ranges;
        }

        int begin = 0;
        int end = 0;
        while (end < ports.length) {

            if (end < ports.length - 1 &&
                    ports[end] + 1 == ports[end + 1]) {
                ++end;
            } else {
                ranges.add(PortRange.create(ports[begin], ports[end]));
                ++end;
                begin = end;
            }
        }
        return ranges;
    }


    /**
     * Consturcts a PortRange.
     * @param begin The first port in the range.
     * @param end The last port in the range.
     */
    public PortRange(int begin, int end) {
        this.begin = begin;
        this.end = end;

    }

    /**
     * Gets the beginning port.
     * @return The first port in the range.
     */
    public int getBegin() {
        return begin;
    }

    /**
     * Gets the end port.
     * @return The last port in the range.
     */
    public int getEnd() {
        return end;
    }

    /**
     * Tests if a PortRange is in the range.
     * @param other The PortRange that will be tested.
     * @return True if other falls withing the range. That is true if,
     * getBegin() <= other.getBegin() && other.getEnd() <= getEnd().
     */
    public boolean contains(PortRange other) {

        return this.begin <= other.begin &&
                other.end <= this.end;
    }

    /**
     * Gets a Protocol Buffers representation of the PortRange
     * @return The Range object containing the PortRange.
     */
    Range toProto() {

        return Range.newBuilder()
                .setBegin(begin)
                .setEnd(end)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PortRange portRange = (PortRange) o;

        if (begin != portRange.begin) return false;
        return end == portRange.end;

    }

    @Override
    public int hashCode() {
        int result = begin;
        result = 31 * result + end;
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
