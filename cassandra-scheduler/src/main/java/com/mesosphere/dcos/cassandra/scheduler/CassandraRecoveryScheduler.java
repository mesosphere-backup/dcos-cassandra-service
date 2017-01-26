package com.mesosphere.dcos.cassandra.scheduler;

import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.offer.OfferEvaluator;
import org.apache.mesos.offer.OfferRecommendation;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.ChainedObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class CassandraRecoveryScheduler extends ChainedObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            CassandraRecoveryScheduler.class);

    private final OfferAccepter offerAccepter;
    private final PersistentOfferRequirementProvider offerRequirementProvider;
    private final CassandraState cassandraState;
    private final OfferEvaluator offerEvaluator;
    private final Random random = new Random();

    public CassandraRecoveryScheduler(
            PersistentOfferRequirementProvider requirementProvider,
            OfferAccepter offerAccepter, CassandraState cassandraState) {
        this.offerAccepter = offerAccepter;
        this.cassandraState = cassandraState;
        this.offerRequirementProvider = requirementProvider;
        this.offerEvaluator = new OfferEvaluator(cassandraState.getStateStore());

        this.cassandraState.subscribe(this);
    }

    public boolean hasOperations() {
        return getTerminatedTask(new HashSet<>()).isPresent();
    }

    public List<Protos.OfferID> resourceOffers(final SchedulerDriver driver,
                                               final List<Protos.Offer> offers,
                                               final Set<String> ignore) {

        List<Protos.OfferID> acceptedOffers = Collections.emptyList();
        Optional<CassandraDaemonTask> terminatedOption = getTerminatedTask(ignore);

        if (terminatedOption.isPresent()) {
            try {
                CassandraDaemonTask terminated = terminatedOption.get();
                terminated = cassandraState.replaceDaemon(terminated);

                Optional<OfferRequirement> offerReq;
                String replaceIp = terminated.getConfig().getReplaceIp();
                if (StringUtils.isEmpty(replaceIp)) {
                    offerReq = offerRequirementProvider.getReplacementOfferRequirement(
                            cassandraState.getOrCreateContainer(terminated.getName()));
                } else {
                    offerReq = offerRequirementProvider.getNewOfferRequirement(
                            cassandraState.createCassandraContainer(terminated.getName(), replaceIp));
                }

                if (offerReq.isPresent()) {
                    LOGGER.info("Attempting to satisfy OfferRequirement: " + offerReq.get());
                    List<OfferRecommendation> recommendations =
                            offerEvaluator.evaluate(offerReq.get(), offers);
                    LOGGER.debug(
                            "Got recommendations: {} for terminated task: {}",
                            recommendations,
                            terminated.getId());
                    acceptedOffers = offerAccepter.accept(driver,
                            recommendations);
                }


            } catch (PersistenceException | ConfigStoreException ex) {
                LOGGER.error(
                        String.format("Persistence error recovering " +
                                "terminated task %s", terminatedOption),
                        ex);
            }
        }
        return acceptedOffers;
    }

    private Optional<CassandraDaemonTask> getTerminatedTask(
            final Set<String> ignore) {
        LOGGER.info("Ignoring steps: {}", ignore);
        cassandraState.refreshTasks();
        List<CassandraDaemonTask> terminated =
                cassandraState.getDaemons().values().stream()
                        .filter(task -> cassandraState.isTerminated(task))
                        .filter(task -> !ignore.contains(task.getName()))
                        .collect(Collectors.toList());
        LOGGER.info("Terminated tasks size: {}", terminated.size());
        if (terminated.size() > 0) {
            return Optional.of(terminated.get(
                    random.nextInt(terminated.size())));
        } else {
            return Optional.empty();
        }
    }

}
