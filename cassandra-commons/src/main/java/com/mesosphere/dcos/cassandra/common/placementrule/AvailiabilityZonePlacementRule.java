package com.mesosphere.dcos.cassandra.common.placementrule;

import java.util.Collection;
import java.util.List;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.constrain.PlacementRule;

public class AvailiabilityZonePlacementRule implements PlacementRule {

	private String availiabilityZone;
	private final static String AVAILIABLITY_ZONE_ATTRIBUTE_NAME = "zone";

	public AvailiabilityZonePlacementRule(String availiabilityZone) {
		this.availiabilityZone = availiabilityZone;
	}

	@Override
	public Offer filter(Offer offer, OfferRequirement offerRequirement, Collection<TaskInfo> tasks) {
		List<Attribute> attributes = offer.getAttributesList();
		for (Attribute attribute : attributes) {
			if (AVAILIABLITY_ZONE_ATTRIBUTE_NAME.equals(attribute.getName())) {
				if (attribute.getText().getValue().equals(availiabilityZone)) {
					return offer;
				}
			}
		}
		return offer.toBuilder().clearResources().build();
	}

	@Override
	public String toString() {
		return "AvailiabilityZonePlacementRule [availiabilityZone=" + availiabilityZone + "]";
	}
	
	
}
