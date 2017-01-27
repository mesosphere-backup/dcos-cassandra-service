package com.mesosphere.dcos.cassandra.scheduler.seeds;


import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.resources.SeedsResponse;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.SerializationUtils;
import org.apache.mesos.state.JsonSerializer;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SeedsManager implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SeedsManager.class);
    private static final JsonSerializer DATACENTER_SERIALIZER = new JsonSerializer();

    private final CassandraState tasks;
    private static final String DATA_CENTERS_KEY = "datacenters";
    private volatile ImmutableMap<String, DataCenterInfo> dataCenters;
    private final SchedulerClient client;
    private DefaultConfigurationManager configurationManager;
    private final StateStore stateStore;

    private boolean putLocalInfo(String url) {
        try {
            DataCenterInfo local = getLocalInfo();
            LOGGER.info("Registering local data center info: info = {}, url = {}", local, url);

            boolean success = client.putDataCenterInfo(url, local) .toCompletableFuture().get();
            if (success) {
                LOGGER.info("Registered local data center info: info = {}, url = {}", local, url);
            } else {
                LOGGER.error("Failed to register local data center info: info = {}, url = {}", local, url);
            }
            return success;
        } catch (Throwable t) {
            LOGGER.error(String.format("Error registering local data center info : url = %s", url), t);
            return false;
        }
    }

    Optional<DataCenterInfo> getRemoteInfo(String url) {
        LOGGER.info("Retrieving info: url = {}", url);
        try {
            DataCenterInfo info = client.getDataCenterInfo(url).toCompletableFuture().get();
            LOGGER.info("Retrieved data center info: info = {}, url = {}", info, url);
            return Optional.of(info);
        } catch (Throwable t) {
            LOGGER.error(String.format("Failed to retrieve info: url = %s", url), t);
            return Optional.empty();
        }
    }

    @Inject
    public SeedsManager(final DefaultConfigurationManager configurationManager,
                        final CassandraState tasks,
                        final ScheduledExecutorService executor,
                        final SchedulerClient client,
                        final StateStore stateStore) throws ConfigStoreException {
        this.tasks = tasks;
        this.stateStore = stateStore;
        this.configurationManager = configurationManager;
        ImmutableMap.Builder<String, DataCenterInfo> builder =
                ImmutableMap.<String, DataCenterInfo>builder();
        this.client = client;
        try {
            synchronized (this.stateStore) {
                LOGGER.info("Loading data from persistent store");
                for (final String key : stateStore.fetchPropertyKeys()) {
                    if (!key.startsWith(DATA_CENTERS_KEY)) {
                        continue;
                    }
                    LOGGER.info("Loaded key: {}", key);
                    builder.put(key, DATACENTER_SERIALIZER.deserialize(
                            stateStore.fetchProperty(key), DataCenterInfo.class));
                }
                dataCenters = builder.build();
                LOGGER.info("Loaded data centers: {}", SerializationUtils.toJsonString(dataCenters));
            }
        } catch (IOException e) {
            LOGGER.error("Error loading data centers", e);
            throw new RuntimeException(e);
        } catch (StateStoreException e) {
            LOGGER.warn("No backup context found.", e);
        } finally {
            if (dataCenters == null) {
                // Initialize an empty map.
                dataCenters = ImmutableMap.<String, DataCenterInfo>builder().build();
            }
        }
        final CassandraSchedulerConfiguration configuration = (CassandraSchedulerConfiguration)
                configurationManager.getTargetConfig();
        LOGGER.info("Starting synchronization delay = {} ms", configuration.getExternalDcSyncMs());
        executor.scheduleWithFixedDelay(
                this,
                configuration.getExternalDcSyncMs(),
                configuration.getExternalDcSyncMs(),
                TimeUnit.MILLISECONDS);
    }

    public List<String> getLocalSeeds() throws IOException {
        final List<CassandraDaemonTask> active = tasks.getDaemons().values()
                .stream()
                .filter(daemon -> daemon.getMode() == CassandraMode.NORMAL && daemon.getHostname().isEmpty() == false)
                .collect(Collectors.toList());

        for (CassandraDaemonTask daemonTask: tasks.getDaemons().values()) {
           LOGGER.info("DaemonTask mode: {}, hostname: {}", daemonTask.getMode(), daemonTask.getHostname());
        }

        final int seedCount = getConfiguredSeedsCount();
        final List<String> seeds = new ArrayList<>(active.size());

        for (int seed = 0; seed < seedCount && seed < active.size(); ++seed) {
            seeds.add(InetAddress.getByName(active.get(seed).getHostname())
                    .getHostAddress());
        }

        return seeds;
    }

    public int getConfiguredSeedsCount() throws ConfigStoreException {
        return ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getSeeds();
    }

    public List<DataCenterInfo> getDataCenters() {
        return dataCenters.values().asList();
    }

    public List<String> getConfiguredDataCenters() throws ConfigStoreException {
        return ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getExternalDcsList();
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

    public void update(final DataCenterInfo info) throws IOException {
        LOGGER.info("Updating data center {}", info);
        synchronized (stateStore) {
            final String propertyKey = DATA_CENTERS_KEY + "." + info.getDatacenter();
            stateStore.storeProperty(propertyKey, DATACENTER_SERIALIZER.serialize(info));
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
        LOGGER.info("Data centers after update = {},", SerializationUtils.toJsonString(dataCenters));
    }

    public boolean tryUpdate(final DataCenterInfo info) {
        try {
            update(info);
            return true;
        } catch (IOException e) {
            LOGGER.error(
                    String.format("Failed to update info: info = %s",
                            info),
                    e);
            return false;
        }
    }

    public DataCenterInfo getLocalInfo() throws IOException {
        return DataCenterInfo.create(
                ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                        .getCassandraConfig().getLocation().getDataCenter(),
                ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig()).getDcUrl(),
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
