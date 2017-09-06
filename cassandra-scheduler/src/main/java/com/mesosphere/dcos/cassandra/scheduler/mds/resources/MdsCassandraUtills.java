package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class MdsCassandraUtills {

    public static String getDataCenterVsReplicationFactorString(Map<String, Integer> dcVsRF) {
        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, Integer> entry : dcVsRF.entrySet()) {
            if (!StringUtils.isEmpty(result.toString())) {
                result.append(",");
            }
            result.append("'").append(entry.getKey()).append("'").append(":").append(entry.getValue());
        }
        return result.toString();
    }

}
