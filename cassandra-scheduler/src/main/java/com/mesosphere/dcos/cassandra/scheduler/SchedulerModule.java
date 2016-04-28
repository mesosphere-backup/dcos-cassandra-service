package com.mesosphere.dcos.cassandra.scheduler;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ClusterTaskConfig;
import com.mesosphere.dcos.cassandra.common.serialization.BooleanStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;
import com.mesosphere.dcos.cassandra.scheduler.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPhaseStrategies;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraStageManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Environment;
import org.apache.http.client.HttpClient;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.PhaseStrategyFactory;
import org.apache.mesos.scheduler.plan.StageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SchedulerModule extends AbstractModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(
            SchedulerModule.class);

    private final CassandraSchedulerConfiguration configuration;
    private final Environment environment;

    public SchedulerModule(
            final CassandraSchedulerConfiguration configuration,
            final Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
    }

    @Override
    protected void configure() {
        bind(Environment.class).toInstance(this.environment);

        bind(CassandraSchedulerConfiguration.class).toInstance(
                this.configuration);

        bind(PersistenceFactory.class).toInstance(ZooKeeperPersistence
                .create(
                        configuration.getIdentity(),
                        configuration.getCuratorConfig()));

        bind(new TypeLiteral<Serializer<Integer>>() {
        }).toInstance(IntegerStringSerializer.get());

        bind(new TypeLiteral<Serializer<Boolean>>() {
        }).toInstance(BooleanStringSerializer.get());

        bind(new TypeLiteral<Serializer<Identity>>() {
        }).toInstance(Identity.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<CassandraConfig>>() {
        }).toInstance(CassandraConfig.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<ExecutorConfig>>() {
        }).toInstance(ExecutorConfig.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<CassandraTask>>() {
        }).toInstance(CassandraTask.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<ClusterTaskConfig>>() {
        }).toInstance(ClusterTaskConfig.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<BackupContext>>() {
        }).toInstance(BackupContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<RestoreContext>>() {
        }).toInstance(RestoreContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<CleanupContext>>() {
        }).toInstance(CleanupContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<RepairContext>>() {
        }).toInstance(RepairContext.JSON_SERIALIZER);

        bind(new TypeLiteral<Serializer<DataCenterInfo>>() {
        }).toInstance(
                DataCenterInfo.JSON_SERIALIZER
        );

        bind(MesosConfig.class).toInstance(configuration.getMesosConfig());

        bindConstant().annotatedWith(Names.named("SeedsUrl")).to(
                configuration.getSeedsUrl()
        );
        bindConstant().annotatedWith(Names.named("ConfiguredSyncDelayMs")).to(
                configuration.getExternalDcSyncMs()
        );
        bindConstant().annotatedWith(Names.named("ConfiguredDcUrl")).to(
                configuration.getDcUrl()
        );
        bind(new TypeLiteral<List<String>>() {})
                .annotatedWith(Names.named("ConfiguredExternalDcs"))
                .toInstance(configuration.getExternalDcsList());
        bind(Identity.class).annotatedWith(
                Names.named("ConfiguredIdentity")).toInstance(
                configuration.getIdentity());
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

        bind(HttpClient.class).toInstance(new HttpClientBuilder(environment).using(
                configuration.getHttpClientConfiguration())
                .build("http-client"));
        bind(ExecutorService.class).toInstance(Executors.newCachedThreadPool());
        bind(ScheduledExecutorService.class).toInstance(
                Executors .newScheduledThreadPool(8));
        bind(PhaseStrategyFactory.class).to(CassandraPhaseStrategies.class)
                .asEagerSingleton();
        bind(StageManager.class).to(CassandraStageManager.class)
                .asEagerSingleton();
        bind(SchedulerClient.class).asEagerSingleton();
        bind(IdentityManager.class).asEagerSingleton();
        bind(ConfigurationManager.class).asEagerSingleton();
        bind(PersistentOfferRequirementProvider.class);
        bind(CassandraTasks.class).asEagerSingleton();
        bind(EventBus.class).asEagerSingleton();
        bind(BackupManager.class).asEagerSingleton();
        bind(ClusterTaskOfferRequirementProvider.class);
        bind(Reconciler.class).to(DefaultReconciler.class).asEagerSingleton();
        bind(RestoreManager.class).asEagerSingleton();
        bind(CleanupManager.class).asEagerSingleton();
        bind(RepairManager.class).asEagerSingleton();
        bind(SeedsManager.class).asEagerSingleton();
    }
}
