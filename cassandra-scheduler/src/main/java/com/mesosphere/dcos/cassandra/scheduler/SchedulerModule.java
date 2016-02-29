package com.mesosphere.dcos.cassandra.scheduler;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.mesosphere.dcos.cassandra.common.client.ExecutorClient;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.serialization.BooleanStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import com.mesosphere.dcos.cassandra.scheduler.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceFactory;
import com.mesosphere.dcos.cassandra.scheduler.persistence.ZooKeeperPersistence;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPhaseStrategies;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPlanManager;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraReconciler;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import com.mesosphere.dcos.cassandra.scheduler.tasks.Reconciler;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

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

        bind(CassandraSchedulerConfiguration.class).toInstance(
                this.configuration);

        bind(PersistenceFactory.class).toInstance(ZooKeeperPersistence
                .create(
                        configuration.getIdentity(),
                        configuration.getCuratorConfig()));

        bind(new TypeLiteral<Serializer<Integer>>() {
        }).toInstance(
                IntegerStringSerializer.get());
        bind(new TypeLiteral<Serializer<Boolean>>() {
        }).toInstance(
                BooleanStringSerializer.get());
        bind(new TypeLiteral<Serializer<Identity>>() {
        }).toInstance(Identity.JSON_SERIALIZER);
        bind(new TypeLiteral<Serializer<CassandraConfig>>() {
        }).toInstance(CassandraConfig.JSON_SERIALIZER);
        bind(new TypeLiteral<Serializer<ExecutorConfig>>() {
        }).toInstance(ExecutorConfig.JSON_SERIALIZER);
        bind(new TypeLiteral<Serializer<CassandraTask>>() {
        })
                .toInstance(CassandraTask.JSON_SERIALIZER);

        bind(MesosConfig.class).toInstance(configuration.getMesosConfig());

        bindConstant().annotatedWith(Names.named("SeedsUrl")).to(
                configuration.getSeedsUrl()
        );
        bind(Identity.class).annotatedWith(
                Names.named("ConfiguredIdentity")).toInstance(
                configuration.getIdentity());
        bind(CassandraConfig.class).annotatedWith(
                Names.named("ConfiguredCassandraConfig")).toInstance(
                configuration.getCassandraConfig());

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
                Names.named("ConfiguredUpdateConfig")).to(
                configuration.updateConfig());
        bindConstant().annotatedWith(
                Names.named("ConfiguredPlacementStrategy")).to(
                configuration.getPlacementStrategy());

        bindConstant().annotatedWith(
                Names.named("ConfiguredPlanStrategy")).to(
                configuration.getPlanStrategy());
        bindConstant().annotatedWith(
                Names.named("ConfiguredPhaseStrategy")).to(
                configuration.getPhaseStrategy()
        );
        bind(CassandraPhaseStrategies.class).asEagerSingleton();
        bind(CassandraPlanManager.class).asEagerSingleton();
        bind(ExecutorClient.class).toInstance(ExecutorClient.create(
                new HttpClientBuilder(environment).using(
                        configuration.getHttpClientConfiguration())
                        .build("executor-http-client"),
                Executors.newCachedThreadPool()
        ));
        bind(IdentityManager.class).asEagerSingleton();
        bind(ConfigurationManager.class).asEagerSingleton();
        bind(PersistentOfferRequirementProvider.class);
        bind(CassandraTasks.class).asEagerSingleton();
        bind(EventBus.class).asEagerSingleton();
        bind(Reconciler.class).to(CassandraReconciler.class).asEagerSingleton();
    }
}
