package com.mesosphere.dcos.cassandra.scheduler;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.serialization.BooleanStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.BackupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.backup.RestoreManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.cleanup.CleanupManager;
import com.mesosphere.dcos.cassandra.scheduler.plan.repair.RepairManager;
import com.mesosphere.dcos.cassandra.scheduler.seeds.SeedsManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.http.client.HttpClient;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TestModule extends AbstractModule {
    private final CassandraSchedulerConfiguration configuration;
    private final CuratorFrameworkConfig curatorConfig;
    private final MesosConfig mesosConfig;
    private final Environment environment;

    public static TestingServer createTestingServerQuietly() {
        try {
            return new TestingServer(40000);
        } catch (Exception ex) {
            return null;
        }
    }

    public static CuratorFramework createClient(final TestingServer server) {
        return CuratorFrameworkFactory.newClient(
                server.getConnectString(), new RetryOneTime(1));
    }

    public TestModule(
            final CassandraSchedulerConfiguration configuration,
            final CuratorFrameworkConfig curatorConfig,
            final MesosConfig mesosConfig,
            final Environment environment) {
        this.configuration = configuration;
        this.curatorConfig = curatorConfig;
        this.mesosConfig = mesosConfig;
        this.environment = environment;
    }

    @Override
    protected void configure() {
        bind(CassandraSchedulerConfiguration.class).toInstance(this.configuration);
        bind(new TypeLiteral<Serializer<Integer>>() {}).toInstance(IntegerStringSerializer.get());
        bind(new TypeLiteral<Serializer<Boolean>>() {}).toInstance(BooleanStringSerializer.get());
        bind(new TypeLiteral<Serializer<CassandraTask>>() {}).toInstance(CassandraTask.PROTO_SERIALIZER);
        bind(MesosConfig.class).toInstance(mesosConfig);

        bind(ServiceConfig.class)
                .annotatedWith(Names.named("ConfiguredIdentity"))
                .toInstance(configuration.getServiceConfig());
        bindConstant()
                .annotatedWith(Names.named("ConfiguredEnableUpgradeSSTableEndpoint"))
                .to(configuration.getEnableUpgradeSSTableEndpoint());

        HttpClientConfiguration httpClient = new HttpClientConfiguration();
        bind(HttpClient.class)
                .toInstance(new HttpClientBuilder(environment).using(httpClient).build("http-client-test"));
        bind(ExecutorService.class).toInstance(Executors.newCachedThreadPool());
        bind(CuratorFrameworkConfig.class).toInstance(curatorConfig);
        bind(ClusterTaskConfig.class).toInstance(configuration.getClusterTaskConfig());
        bind(ScheduledExecutorService.class).toInstance(Executors.newScheduledThreadPool(8));
        bind(SchedulerClient.class).asEagerSingleton();
        bind(IdentityManager.class).asEagerSingleton();
        bind(ConfigurationManager.class).asEagerSingleton();
        bind(PersistentOfferRequirementProvider.class);
        bind(CassandraState.class).asEagerSingleton();
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
