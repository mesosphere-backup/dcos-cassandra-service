package com.mesosphere.dcos.cassandra.scheduler.offer;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OperationRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogOperationRecorder implements OperationRecorder {

    private final static Logger LOGGER = LoggerFactory.getLogger(
            LogOperationRecorder.class);

    public void record(Protos.Offer.Operation operation,
                       Protos.Offer offer) throws Exception {
        LOGGER.debug("Offer: {}", offer);
        LOGGER.debug("Operation: {}", operation);
    }
}