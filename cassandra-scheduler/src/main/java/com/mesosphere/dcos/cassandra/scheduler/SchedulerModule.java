package com.mesosphere.dcos.cassandra.scheduler;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.metrics.StatsDMetrics;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.serialization.BooleanStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.IntegerStringSerializer;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Environment;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.http.client.HttpClient;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.dcos.DcosCluster;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.api.JsonPropertyDeserializer;
import org.apache.mesos.state.api.PropertyDeserializer;

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
        bind(CassandraSchedulerConfiguration.class).toInstance(this.configuration);

        RetryPolicy retryPolicy =
                curatorConfig.getOperationTimeout().isPresent() ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs().get().intValue(),
                                (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        CuratorStateStore curatorStateStore = new CuratorStateStore(
                configuration.getServiceConfig().getName(),
                curatorConfig.getServers(),
                retryPolicy);
        bind(StateStore.class).toInstance(curatorStateStore);

        StatsDMetrics metrics = new StatsDMetrics(
                configuration.getMetricConfig());
        bind(StatsDMetrics.class).toInstance(metrics);

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

        bind(new TypeLiteral<Serializer<Integer>>() {}).toInstance(IntegerStringSerializer.get());
        bind(new TypeLiteral<Serializer<Boolean>>() {}).toInstance(BooleanStringSerializer.get());
        bind(new TypeLiteral<Serializer<CassandraTask>>() {}).toInstance(CassandraTask.PROTO_SERIALIZER);

        bind(MesosConfig.class).toInstance(mesosConfig);

        // Annotated bindings:
        bind(ServiceConfig.class)
                .annotatedWith(Names.named("ConfiguredIdentity"))
                .toInstance(configuration.getServiceConfig());
        bindConstant()
                .annotatedWith(Names.named("ConfiguredEnableUpgradeSSTableEndpoint"))
                .to(configuration.getEnableUpgradeSSTableEndpoint());

        bind(HttpClient.class).toInstance(new HttpClientBuilder(environment).using(
                configuration.getHttpClientConfiguration()).build("http-client"));
        bind(ExecutorService.class).toInstance(Executors.newCachedThreadPool());
        bind(CuratorFrameworkConfig.class).toInstance(curatorConfig);
        bind(ClusterTaskConfig.class).toInstance(configuration.getClusterTaskConfig());
        bind(ScheduledExecutorService.class).toInstance(Executors.newScheduledThreadPool(8));
        bind(SchedulerClient.class).asEagerSingleton();
        bind(IdentityManager.class).asEagerSingleton();
        bind(ConfigurationManager.class).asEagerSingleton();
        bind(PersistentOfferRequirementProvider.class);
        bind(CassandraState.class).asEagerSingleton();
        bind(ClusterTaskOfferRequirementProvider.class);
        bind(PropertyDeserializer.class).to(JsonPropertyDeserializer.class);
    }
}
