package com.mesosphere.dcos.cassandra.common.offer;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;

public interface CassandraOfferRequirementProvider {

    OfferRequirement getNewOfferRequirement(String type, Protos.TaskInfo taskInfo);

    OfferRequirement getReplacementOfferRequirement(String type, Protos.TaskInfo taskInfo);

    OfferRequirement getUpdateOfferRequirement(String type, Protos.TaskInfo info);
}
