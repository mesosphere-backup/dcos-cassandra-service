package com.mesosphere.dcos.cassandra.common.placementrule;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.constrain.PlacementRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;

public class AvailiabilityZonePlacementRule implements PlacementRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvailiabilityZonePlacementRule.class);
    private final static String AVAILIABLITY_ZONE_ATTRIBUTE_NAME = "zone";
    private DefaultConfigurationManager configurationManager;

    public AvailiabilityZonePlacementRule(DefaultConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    @Override
    public Offer filter(Offer offer, OfferRequirement offerRequirement, Collection<TaskInfo> tasks) {
        CassandraSchedulerConfiguration schedulerConfiguration;
        List<Attribute> attributes = offer.getAttributesList();
        String hostname = offer.getHostname();
        boolean azKnown = false;
        try {
            schedulerConfiguration = (CassandraSchedulerConfiguration) configurationManager.getTargetConfig();
        } catch (ConfigStoreException e) {
            LOGGER.error("Scheduler config manager read failed, will reject this offer:" + e.getMessage(), e);
            return offer.toBuilder().clearResources().build();
        }
        LOGGER.info("AvailiabilityZonePlacementRule filter with host: {}", hostname);
        for (Attribute attribute : attributes) {
            if (AVAILIABLITY_ZONE_ATTRIBUTE_NAME.equals(attribute.getName())) {
                String zones = schedulerConfiguration.getZones();
                if(StringUtils.isEmpty(zones)){
                    azKnown = false;
                } else {
                    azKnown = true;
                }
                if(isValidOffer(zones,hostname,attribute.getText().getValue())) {
                    return offer;
                }
            }
        }
        if(!azKnown){
            LOGGER.info("Az unknow , so not doing any filter");
            return offer;
        }
        return offer.toBuilder().clearResources().build();
    }

    private boolean isValidOffer(String zones, String hostname, String zoneName) {
        //Its in formatter node-0:AZ:hostip,node-1:AZ:hostIp
        if(StringUtils.isEmpty(zones)){
            return false;
        }
        String[] azHostMap = zones.split(",");
        for(String text :azHostMap) {
            if(text.contains(hostname) && text.contains(zoneName)) {
                //Matches can be validation with split with ':' then match with [0] and [1]
                //text.split(":"); and then can be matched with exact string
                LOGGER.info("Found match for az :"+text);
                return true;
            }
        }
        
        return false;
    }

}
