package com.mesosphere.dcos.cassandra.scheduler.offer;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;

public interface CassandraOfferRequirementProvider {

    OfferRequirement getNewOfferRequirement(Protos.TaskInfo taskInfo);

    OfferRequirement getReplacementOfferRequirement(Protos.TaskInfo taskInfo);

    OfferRequirement getUpdateOfferRequirement(Protos.TaskInfo info);
}
