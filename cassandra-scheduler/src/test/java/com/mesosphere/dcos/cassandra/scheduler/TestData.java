package com.mesosphere.dcos.cassandra.scheduler;

import org.apache.mesos.Protos;

import java.util.List;
import java.util.UUID;

public class TestData {

    public static String randomId() {
        return UUID.randomUUID().toString();
    }

    public static Protos.Offer createOffer(String id,
                                           String slave,
                                           String framework,
                                           String hostname,
                                           List<Protos.Resource> resources) {

        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(id))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slave))
                .setHostname(hostname)
                .setFrameworkId(Protos.FrameworkID.newBuilder()
                        .setValue(framework))
                .addAllResources(resources).build();
    }
}

