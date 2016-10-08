package com.mesosphere.dcos.cassandra.scheduler;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.common.serialization.BooleanStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPhaseStrategies;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPlanManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraState;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.setup.Environment;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.http.client.HttpClient;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.dcos.DcosCluster;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.reconciliation.TaskStatusProvider;
import org.apache.mesos.scheduler.plan.PhaseStrategyFactory;
import org.apache.mesos.scheduler.plan.PlanManager;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.api.JsonPropertyDeserializer;
import org.apache.mesos.state.api.PropertyDeserializer;

import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SchedulerModule extends AbstractModule {

    private final CassandraSchedulerConfiguration configuration;
    private final Environment environment;
    private final CuratorFrameworkConfig curatorConfig;
    private final MesosConfig mesosConfig;

    public SchedulerModule(
            final CassandraSchedulerConfiguration configuration,
            final CuratorFrameworkConfig curatorConfig,
            final MesosConfig mesosConfig,
            final Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
        this.curatorConfig = curatorConfig;
        this.mesosConfig = mesosConfig;
    }

    @Override
    protected void configure() {
        bind(Environment.class).toInstance(this.environment);

        bind(CassandraSchedulerConfiguration.class).toInstance(
                this.configuration);


        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        CuratorStateStore curatorStateStore = new CuratorStateStore(
                configuration.getServiceConfig().getName(),
                curatorConfig.getServers(),
                retryPolicy);
        bind(StateStore.class).toInstance(curatorStateStore);

        try {
            Capabilities capabilities = new Capabilities(new DcosCluster());
            bind(Capabilities.class).toInstance(capabilities);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }

        try {
            final ConfigValidator configValidator = new ConfigValidator();
            final DefaultConfigurationManager configurationManager =
                    new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                    configuration.getServiceConfig().getName(),
                    curatorConfig.getServers(),
                    configuration,
                    configValidator,
                    curatorStateStore);
            bind(DefaultConfigurationManager.class).toInstance(configurationManager);
        } catch (ConfigStoreException e) {
            throw new RuntimeException(e);
        }

        bind(new TypeLiteral<Serializer<Integer>>() {
        }).toInstance(IntegerStringSerializer.get());

        bind(new TypeLiteral<Serializer<Boolean>>() {
        }).toInstance(BooleanStringSerializer.get());

        bind(new TypeLiteral<Serializer<ServiceConfig>>() {
        }).toInstance(ServiceConfig.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<CassandraConfig>>() {
        }).toInstance(CassandraConfig.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<ExecutorConfig>>() {
        }).toInstance(ExecutorConfig.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<CassandraTask>>() {
        }).toInstance(CassandraTask.PROTO_SERIALIZER);

        bind(new TypeLiteral<Serializer<ClusterTaskConfig>>() {
        }).toInstance(ClusterTaskConfig.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<BackupRestoreContext>>() {
        }).toInstance(BackupRestoreContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<BackupRestoreContext>>() {
        }).toInstance(BackupRestoreContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<CleanupContext>>() {
        }).toInstance(CleanupContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<RepairContext>>() {
        }).toInstance(RepairContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<DataCenterInfo>>() {
        }).toInstance(
                DataCenterInfo.JSON_SERIALIZER
        );

        bind(MesosConfig.class).toInstance(mesosConfig);

        bindConstant().annotatedWith(Names.named("ConfiguredSyncDelayMs")).to(
                configuration.getExternalDcSyncMs()
        );
        bindConstant().annotatedWith(Names.named("ConfiguredDcUrl")).to(
                configuration.getDcUrl()
        );
        bind(new TypeLiteral<List<String>>() {
        })
                .annotatedWith(Names.named("ConfiguredExternalDcs"))
                .toInstance(configuration.getExternalDcsList());
        bind(ServiceConfig.class).annotatedWith(
                Names.named("ConfiguredIdentity")).toInstance(
                configuration.getServiceConfig());
        bind(CassandraConfig.class).annotatedWith(
                Names.named("ConfiguredCassandraConfig")).toInstance(
                configuration.getCassandraConfig());
        bind(ClusterTaskConfig.class).annotatedWith(
                Names.named("ConfiguredClusterTaskConfig")).toInstance(
                configuration.getClusterTaskConfig());
        bind(ExecutorConfig.class).annotatedWith(
                Names.named("ConfiguredExecutorConfig")).toInstance(
                configuration.getExecutorConfig());
        bindConstant().annotatedWith(
                Names.named("ConfiguredServers")).to(
                configuration.getServers());
        bindConstant().annotatedWith(
                Names.named("ConfiguredSeeds")).to(
                configuration.getSeeds());
        bindConstant().annotatedWith(
                Names.named("ConfiguredPlacementStrategy")).to(
                configuration.getPlacementStrategy());
        bindConstant().annotatedWith(
                Names.named("ConfiguredPhaseStrategy")).to(
                configuration.getPhaseStrategy()
        );

        HttpClientConfiguration httpClient = new HttpClientConfiguration();
        bind(HttpClient.class).toInstance(new HttpClientBuilder(environment).using(httpClient).build("http-client"));
        bind(ExecutorService.class).toInstance(Executors.newCachedThreadPool());
        bind(CuratorFrameworkConfig.class).toInstance(curatorConfig);
        bind(ClusterTaskConfig.class).toInstance(configuration.getClusterTaskConfig());
        bind(ScheduledExecutorService.class).toInstance(
                Executors .newScheduledThreadPool(8));
        bind(PhaseStrategyFactory.class).to(CassandraPhaseStrategies.class)
                .asEagerSingleton();
        bind(PlanManager.class).to(CassandraPlanManager.class)
                .asEagerSingleton();
        bind(SchedulerClient.class).asEagerSingleton();
        bind(IdentityManager.class).asEagerSingleton();
        bind(ConfigurationManager.class).asEagerSingleton();
        bind(PersistentOfferRequirementProvider.class);
        bind(CassandraState.class).asEagerSingleton();
        bind(TaskStatusProvider.class).to(CassandraState.class);
        bind(EventBus.class).asEagerSingleton();
        bind(BackupManager.class).asEagerSingleton();
        bind(ClusterTaskOfferRequirementProvider.class);
        try {
            bind(Reconciler.class).toConstructor(DefaultReconciler.class.getConstructor(TaskStatusProvider.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        bind(RestoreManager.class).asEagerSingleton();
        bind(CleanupManager.class).asEagerSingleton();
        bind(RepairManager.class).asEagerSingleton();
        bind(SeedsManager.class).asEagerSingleton();
        bind(PropertyDeserializer.class).to(JsonPropertyDeserializer.class);
    }
}
