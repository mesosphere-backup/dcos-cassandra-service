package com.mesosphere.dcos.cassandra.common.tasks.repair;

import com.google.common.collect.Iterators;

import org.apache.mesos.state.JsonSerializer;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RepairContextTest {
    @Test
    public void testJSONSerializationWithSnakeCaseMembers() throws Exception {
        RepairContext context = new RepairContext(
                Arrays.asList("node1"), Arrays.asList("keyspace1"), Arrays.asList("column_family1"));
        ObjectMapper om = new ObjectMapper();

        JsonSerializer serializer = new JsonSerializer();
        String jsonContext = new String(serializer.serialize(context), StandardCharsets.UTF_8);

        JsonNode rehydratedContext = om.readTree(jsonContext);
        List<String> keys = new ArrayList<>();
        Iterators.addAll(keys, rehydratedContext.getFieldNames());
        keys.sort(String::compareTo);

        Assert.assertEquals(Arrays.asList("column_families", "key_spaces", "nodes"), keys);

        context = serializer.deserialize(jsonContext.getBytes(StandardCharsets.UTF_8), RepairContext.class);
        Assert.assertEquals(Arrays.asList("column_family1"), context.getColumnFamilies());
        Assert.assertEquals(Arrays.asList("keyspace1"), context.getKeySpaces());
        Assert.assertEquals(Arrays.asList("node1"), context.getNodes());
    }
}