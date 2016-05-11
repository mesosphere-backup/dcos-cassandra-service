package com.mesosphere.dcos.cassandra.scheduler.seeds;


import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistentMap;
import com.mesosphere.dcos.cassandra.scheduler.resources.SeedsResponse;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SeedsManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SeedsManager.class);

    private final ConfigurationManager configuration;
    private final CassandraTasks tasks;
    private static final String DATA_CENTERS_KEY = "datacenters";
    private final PersistentMap<DataCenterInfo> persistent;
    private volatile ImmutableMap<String, DataCenterInfo> dataCenters;
    private final ScheduledExecutorService executor;
    private final SchedulerClient client;
    private boolean putLocalInfo(String url) {
        try {
            DataCenterInfo local = getLocalInfo();
            LOGGER.info(
                    "Registering local data center info: info = {}, " +
                            "url = {}",
                    local,
                    url);

            boolean success =
                    client.putDataCenterInfo(url, local)
                            .toCompletableFuture().get();
            if (success) {
                LOGGER.info(
                        "Registered local data center info: info = {}, " +
                                "url = {}",
                        local,
                        url);
            } else {
                LOGGER.error(
                        "Failed to register local data center info: info = " +
                                "{}," +
                                " url = {}",
                        local,
                        url);
            }
            return success;
        } catch (Throwable t) {
            LOGGER.error(
                    String.format("Error registering local data center info :" +
                            " " +
                            "url = %s", url),
                    t);
            return false;
        }
    }

    Optional<DataCenterInfo> getRemoteInfo(String url) {
        LOGGER.info("Retrieving info: url = {}", url);
        try {
            DataCenterInfo info = client.getDataCetnerInfo(url)
                    .toCompletableFuture()
                    .get();
            LOGGER.info("Retrieved data center info: info = {}, url = {}",
                    info, url);
            return Optional.of(info);
        } catch (Throwable t) {
            LOGGER.error(String.format("Failed to retrieve info: url = %s",
                    url),
                    t);

            return Optional.empty();
        }
    }

    @Inject
    public SeedsManager(final ConfigurationManager configuration,
                        final CassandraTasks tasks,
                        final PersistenceFactory persistence,
                        final Serializer<DataCenterInfo> serializer,
                        final ScheduledExecutorService executor,
                        final SchedulerClient client) {
        this.configuration = configuration;
        this.tasks = tasks;
        persistent = persistence.createMap(DATA_CENTERS_KEY, serializer);
        ImmutableMap.Builder<String, DataCenterInfo> builder =
                ImmutableMap.<String, DataCenterInfo>builder();
        this.executor = executor;
        this.client = client;
        try {
            synchronized (persistent) {
                LOGGER.info("Loading data from persistent store");
                for (String key : persistent.keySet()) {
                    LOGGER.info("Loaded key: {}", key);
                    builder.put(key, persistent.get(key).get());
                }
                dataCenters = builder.build();
                LOGGER.info("Loaded data centers: {}",
                        JsonUtils.toJsonString(dataCenters));

            }
        } catch (PersistenceException e) {
            LOGGER.error("Error loading data centers", e);
            throw new RuntimeException(e);
        }
        LOGGER.info("Starting synchronization delay = {} ms",
                configuration.getDataCenterSyncDelayMs());
        executor.scheduleWithFixedDelay(
                this,
                configuration.getDataCenterSyncDelayMs(),
                configuration.getDataCenterSyncDelayMs(),
                TimeUnit.MILLISECONDS);
    }

    public List<String> getLocalSeeds() throws IOException {
        final List<CassandraDaemonTask> active = tasks.getDaemons().values()
                .stream()
                .filter(daemon -> daemon.getStatus().getMode() ==
                        CassandraMode.NORMAL &&
                        daemon.getHostname().isEmpty() == false).collect(
                        Collectors
                                .toList());

        final int seedCount = configuration.getSeeds();
        final List<String> seeds = new ArrayList<>(active.size());

        for (int seed = 0; seed < seedCount && seed < active.size(); ++seed) {
            seeds.add(InetAddress.getByName(active.get(seed).getHostname())
                    .getHostAddress());
        }

        return seeds;
    }

    public int getConfiguredSeedsCount() {
        return configuration.getSeeds();
    }

    public List<DataCenterInfo> getDataCenters() {
        return dataCenters.values().asList();
    }

    public List<String> getConfiguredDataCenters() {
        return configuration.getExternalDataCenters();
    }

    public boolean sync(String url) {
        LOGGER.info("Syncing seeds with data center: url = {}", url);
        if (putLocalInfo(url)) {
            Optional<DataCenterInfo> remote = getRemoteInfo(url);
            if (remote.isPresent() && tryUpdate(remote.get())) {
                return true;
            }
        }
        return false;
    }

    public List<String> getRemoteSeeds() {
        return dataCenters.values().stream().map(
                dc -> dc.getSeeds()).flatMap
                (List::stream).collect(Collectors.toList());
    }

    public void update(final DataCenterInfo info) throws PersistenceException {
        LOGGER.info("Updating data center {}", info);
        synchronized (persistent) {
            persistent.put(info.getDatacenter(), info);
            dataCenters = ImmutableMap.<String, DataCenterInfo>builder().putAll(
                    dataCenters.entrySet().stream()
                            .filter(entry -> !entry.getKey().equals(
                                    info.getDatacenter()))
                            .collect(Collectors.toMap(
                                    entry -> entry.getKey(),
                                    entry -> entry.getValue())))
                    .put(info.getDatacenter(), info)
                    .build();

        }
        LOGGER.info("Data centers after update = {},",
                JsonUtils.toJsonString(dataCenters));
    }

    public boolean tryUpdate(final DataCenterInfo info) {
        try {
            update(info);
            return true;
        } catch (PersistenceException e) {
            LOGGER.error(
                    String.format("Failed to update info: info = %s",
                            info),
                    e);
            return false;
        }
    }

    public DataCenterInfo getLocalInfo() throws IOException {
        return DataCenterInfo.create(
                configuration.getCassandraConfig().getLocation().getDataCenter(),
                configuration.getDataCenterUrl(),
                getLocalSeeds());
    }

    public SeedsResponse getSeeds() throws IOException {

        List<String> seeds = getLocalSeeds();
        boolean isSeed = seeds.size() < getConfiguredSeedsCount();
        seeds.addAll(getRemoteSeeds());
        return SeedsResponse.create(isSeed, seeds);
    }

    public final void run() {

        for (DataCenterInfo info : dataCenters.values()) {

            try {
                LOGGER.info("Syncing data center {}", info);
                update(getRemoteInfo(info.getUrl()).get());
            } catch (Throwable t) {

                LOGGER.error(String.format("Error syncing data center %s",
                        info.getDatacenter()), t);
            }
        }
    }
}
