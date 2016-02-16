package com.mesosphere.dcos.cassandra.common.serialization;

/**
 * Created by kowens on 2/7/16.
 */
public class BooleanStringSerializer implements Serializer<Boolean> {

    private static final BooleanStringSerializer instance = new
            BooleanStringSerializer();

    public static BooleanStringSerializer get() {return instance;}

    private BooleanStringSerializer(){}
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
