package com.mesosphere.dcos.cassandra.common.tasks.repair;

import com.google.common.collect.Iterators;
import com.mesosphere.dcos.cassandra.common.serialization.JsonSerializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RepairContextTest {
    @Test
    public void testJSONSerializationWithSnakeCaseMembers() throws Exception {
        RepairContext context = new RepairContext(
                Arrays.asList("node1"), Arrays.asList("keyspace1"), Arrays.asList("column_family1"));
        ObjectMapper om = new ObjectMapper();

        String jsonContext = new String(JsonSerializer.create(RepairContext.class).serialize(context), "ISO-8859-1");

        JsonNode rehydratedContext = om.readTree(jsonContext);
        List<String> keys = new ArrayList<>();
        Iterators.addAll(keys, rehydratedContext.getFieldNames());
        keys.sort(String::compareTo);

        Assert.assertEquals(Arrays.asList("column_families", "key_spaces", "nodes"), keys);

        context = JsonUtils.MAPPER.readValue(jsonContext, RepairContext.class);
        Assert.assertEquals(Arrays.asList("column_family1"), context.getColumnFamilies());
        Assert.assertEquals(Arrays.asList("keyspace1"), context.getKeySpaces());
        Assert.assertEquals(Arrays.asList("node1"), context.getNodes());
    }
}