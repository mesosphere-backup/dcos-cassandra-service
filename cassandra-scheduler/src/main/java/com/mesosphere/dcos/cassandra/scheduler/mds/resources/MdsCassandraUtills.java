package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.dcos.Capabilities;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.resources.ConnectionResource;

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
    
    public static Session getSession(final CassandraAuth cassandraAuth, final Capabilities capabilities, final CassandraState state, final ConfigurationManager configurationManager) throws ConfigStoreException {
        final ConnectionResource connectionResource = new ConnectionResource(capabilities, state, configurationManager);
        List<String> connectedNodes = connectionResource.connectAddress();
        
        List<InetSocketAddress> addList = new ArrayList<>();
        for(String connection:connectedNodes){
            String[] hostAndPort = connection.split(":");
            InetSocketAddress addresses = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
            addList.add(addresses);
        }
        
        Cluster cluster = Cluster.builder().addContactPointsWithPorts(addList)
                        .withCredentials(cassandraAuth.getUsername(), cassandraAuth.getPassword()).build();
        Session session = cluster.connect();
        return session;
    }

}
