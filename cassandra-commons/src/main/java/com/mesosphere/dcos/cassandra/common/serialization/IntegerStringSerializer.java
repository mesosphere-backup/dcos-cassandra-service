package com.mesosphere.dcos.cassandra.common.serialization;

public class IntegerStringSerializer implements Serializer<Integer> {

    private static final IntegerStringSerializer instance = new
            IntegerStringSerializer();

    public static IntegerStringSerializer get() {return instance;}

    private IntegerStringSerializer(){}

    @Override
    public byte[] serialize(Integer value) throws SerializationException {
        return value.toString().getBytes();
    }

    @Override
    public Integer deserialize(byte[] bytes)
            throws SerializationException {
        return Integer.parseInt(new String(bytes));
    }
}
