package com.mesosphere.dcos.cassandra.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/**
 * Created by kowens on 2/8/16.
 */
public class JsonUtils {

    private JsonUtils(){}

    public static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory()).registerModule(new GuavaModule())
            .registerModule(new Jdk8Module());

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new GuavaModule())
            .registerModule(new Jdk8Module());


    public static <T> String toJsonString(T value){

        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
           return "";
        }
    }
}
