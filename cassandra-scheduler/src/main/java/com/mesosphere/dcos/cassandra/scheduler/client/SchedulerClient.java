package com.mesosphere.dcos.cassandra.scheduler.client;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import com.mesosphere.dcos.cassandra.scheduler.seeds.DataCenterInfo;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.mesos.config.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class SchedulerClient {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SchedulerClient.class);
    private static final String SCHEME = "http";

    private static final String host(String hostname, int port) {
        return hostname + ":" + port;
    }


    private static final boolean isSuccessful(HttpResponse response) {
        int status = response.getStatusLine().getStatusCode();
        return (200 <= status && status < 300);
    }

    private static <T> CompletableFuture<T> failure(Throwable error) {
        CompletableFuture<T> failed = new CompletableFuture<>();
        failed.completeExceptionally(error);
        return failed;
    }

    public static final SchedulerClient create(
            final HttpClient client,
            final ExecutorService service) {
        return new SchedulerClient(client, service);
    }

    private final HttpClient client;
    private final ExecutorService executor;

    @Inject
    public SchedulerClient(final HttpClient httpClient,
                           final ExecutorService executor) {
        this.client = httpClient;
        this.executor = executor;
    }

    private <T> CompletionStage<T> get(String host,
                                       String path,
                                       Class<T> clazz) {

        try {
            return get(new URIBuilder()
                    .setScheme(SCHEME)
                    .setHost(host)
                    .setPath(path)
                    .build().toString(), clazz);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                    "Get request failed: host = %s, path = %s",
                    host,
                    path),
                    t);
            return failure(t);
        }
    }

    private <T> CompletionStage<T> get(String url, Class<T>
            clazz) {
        LOGGER.debug("Executing get: url = {}", url);
        CompletableFuture<T> promise = new CompletableFuture<>();
        executor.submit(() -> {
            HttpGet get = new HttpGet(url);
            try {
                HttpResponse response = client.execute(get);
                if (!isSuccessful(response)) {
                    LOGGER.error("Get request failed: url = {}, status = {}",
                            url,
                            response.getStatusLine().getStatusCode());
                    promise.completeExceptionally(
                            new SchedulerClientException("Client request " +
                                    "failed status = " + response
                                    .getStatusLine().getStatusCode()));
                } else {
                    promise.complete(SerializationUtils.fromJsonString(
                            IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8),
                            clazz));
                }
            } catch (Throwable t) {
                LOGGER.error(String.format("Get request failed: url = %s",
                        url),
                        t);
                promise.completeExceptionally(t);
            } finally {
                get.releaseConnection();
            }
        });
        return promise;
    }

    private CompletionStage<Boolean> put(String url, Object json) {
        LOGGER.debug("Executing put: url = {}, data = {}", url, json);
        CompletableFuture<Boolean> promise = new CompletableFuture<Boolean>();
        executor.submit(() -> {
            HttpPut put = new HttpPut(url);
            try {
                put.setEntity(new StringEntity(
                        SerializationUtils.toJsonString(json),
                        ContentType.APPLICATION_JSON));
                HttpResponse response = client.execute(put);
                boolean successful = isSuccessful(response);
                if (!successful) {
                    LOGGER.error("Put request failed :url = {}, " +
                                    "status = {}", url,
                            response.getStatusLine().getStatusCode());

                }
                promise.complete(successful);
            } catch (Throwable t) {
                LOGGER.error(String.format("Put request failed: url = %s",
                        url),
                        t);
                promise.completeExceptionally(t);
            } finally{
                put.releaseConnection();
            }
        });
        return promise;
    }

    public CompletionStage<CassandraStatus> status(String hostname, int port) {
        return get(host(hostname, port), "/v1/cassandra/status", CassandraStatus
                .class);
    }

    public CompletionStage<CassandraConfig> configuration(
            String hostname,
            int port) {
        return get(host(hostname, port), "/v1/cassandra/configuration",
                CassandraConfig.class);
    }

    public CompletionStage<DataCenterInfo> getDataCenterInfo(String url) {
        return get(url, DataCenterInfo.class);
    }

    public CompletionStage<Boolean> putDataCenterInfo(
            String url,
            DataCenterInfo info) {

        return put(url, info);
    }
}
