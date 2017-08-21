package com.mesosphere.dcos.cassandra.scheduler.resources;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;

@Path("/v1/matrix")
@Produces(MediaType.APPLICATION_JSON)
public class ServiceMatrixResource {

    private static final Logger LOGGER = LoggerFactory.getLogger
                    (ServiceMatrixResource.class);
    private final ConfigurationManager configurationManager;

    @Inject
    public ServiceMatrixResource(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    @GET
    @Path("/status")
    public MatrixStatusResponse getStatus() throws Exception {
        LOGGER.info("Fetching Matrix Status for the cassandra with localhost , Make sure cassandra service is running on one of cassandra node");
        Map<String,MatrixStatus> result = new HashMap<String, MatrixStatus>();
        Map<String, String> loadMap = new HashMap<>();
        NodeProbe probe = new NodeProbe("127.0.0.1");
        LOGGER.info("Node probe connected with JMX");
        
        loadMap = probe.getLoadMap();
        Map<InetAddress, Float> ownership = probe.getOwnership();
        
        for(Map.Entry<InetAddress, Float> entry :ownership.entrySet()) {
            LOGGER.info("InetAddress"+entry);
            InetAddress key = entry.getKey();
            MatrixStatus status = new MatrixStatus();
            status.setHostName(key.getHostName());
            status.setHostAddress(key.getHostAddress());
            status.setOwnsPercentage((entry.getValue()));
            result.put(key.getHostAddress(), status);
            LOGGER.info("hostname:"+key.getHostName()+ ", hostaddress:"+key.getHostAddress()+ ", address"+key.getAddress()+ ",");
            LOGGER.info("after status"+status);
        }
        
    for(Map.Entry<String, String> entry :loadMap.entrySet()) {
        if(result.containsKey(entry.getKey())) {
            result.get(entry.getKey()).setLoad(entry.getValue());
        }
    }
        
        return getMatrixResponse(result);
    }

    private MatrixStatusResponse getMatrixResponse(Map<String, MatrixStatus> result) {
       
        MatrixStatusResponse response = new MatrixStatusResponse();
        for(Map.Entry<String , MatrixStatus> entry: result.entrySet()) {
            response.getMatrixStatusList().add(entry.getValue());
        }
        return response;
    }

}
