package com.mesosphere.dcos.cassandra.common.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.mesos.Protos.Value.Range;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public final class PortRange {

    @JsonProperty("begin")
    private final int begin;
    @JsonProperty("end")
    private final int end;

    @JsonCreator
    public static PortRange create(
            @JsonProperty("begin") int begin,
            @JsonProperty("end") int end) {
        return new PortRange(begin, end);
    }

    public static PortRange parse(Range range) {
        return create((int) range.getBegin(), (int) range.getEnd());
    }

    public static List<PortRange> fromPorts(int ... ports) {

        Arrays.sort(ports);
        final LinkedList<PortRange> ranges = new LinkedList<PortRange>();

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


    public PortRange(int begin, int end) {
        this.begin = begin;
        this.end = end;

    }

    public int getBegin() {
        return begin;
    }

    public int getEnd() {
        return end;
    }

    public boolean contains(PortRange other) {

        return this.begin <= other.begin &&
                other.end <= this.end;
    }

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
