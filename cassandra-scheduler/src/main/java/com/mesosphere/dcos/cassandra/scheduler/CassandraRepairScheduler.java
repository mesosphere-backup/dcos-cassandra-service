package com.mesosphere.dcos.cassandra.scheduler;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.offer.OfferEvaluator;
import org.apache.mesos.offer.OfferRecommendation;
import org.apache.mesos.offer.OfferRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CassandraRepairScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraRepairScheduler.class);

    private OfferAccepter offerAccepter;
    private CassandraOfferRequirementProvider offerRequirementProvider;
    private CassandraTasks cassandraTasks;

    public CassandraRepairScheduler(
            CassandraOfferRequirementProvider requirementProvider,
            OfferAccepter offerAccepter, CassandraTasks cassandraTasks) {
        this.offerAccepter = offerAccepter;
        this.cassandraTasks = cassandraTasks;
        this.offerRequirementProvider = requirementProvider;
    }

    public List<Protos.OfferID> resourceOffers(SchedulerDriver driver,
                                               List<Protos.Offer> offers,
                                               CassandraBlock block) {
        List<Protos.OfferID> acceptedOffers = new ArrayList<>();
        List<Protos.TaskInfo> terminatedTasks = getTerminatedTasks(block);

        LOGGER.info("Terminated tasks size: {}", terminatedTasks.size());

        if (terminatedTasks.size() > 0) {
            Protos.TaskInfo terminatedTask = terminatedTasks.get(
                    new Random().nextInt(terminatedTasks.size()));
            OfferRequirement offerReq =
                    offerRequirementProvider.getReplacementOfferRequirement(
                            terminatedTask);
            OfferEvaluator offerEvaluator = new OfferEvaluator(offerReq);
            List<OfferRecommendation> recommendations = offerEvaluator.evaluate(
                    offers);
            LOGGER.debug("Got recommendations: {} for terminated task: {}",
                    recommendations,
                    terminatedTask.getTaskId().getValue());

            acceptedOffers = offerAccepter.accept(driver, recommendations);
        }

        return acceptedOffers;
    }

    private List<Protos.TaskInfo> getTerminatedTasks(CassandraBlock block) {
        final List<Protos.TaskInfo> terminatedTasks = new ArrayList<>();
        final List<CassandraTask> terminatedCassandraTasks = cassandraTasks.getTerminatedTasks();
        terminatedCassandraTasks.stream()
                .filter(task -> (block == null) ? true :
                        task.getId() != block.getTaskId())
                .forEach(cassandraTask -> terminatedTasks.add(
                                cassandraTask.toProto()));
        return terminatedTasks;
    }
}
