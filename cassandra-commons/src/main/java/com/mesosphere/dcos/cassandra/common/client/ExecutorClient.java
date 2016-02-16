package com.mesosphere.dcos.cassandra.common.client;

import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class ExecutorClient {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ExecutorClient.class);
    private static final String SCHEME = "http";
    private static final String VERSION = "/v1";

    private static final String host(String hostname, int port) {
        return hostname + ":" + port;
    }

    private static final String command(String command) {
        return "/cassandra/commands/" + command;
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

    public static final ExecutorClient create(final HttpClient client,
                                              final ExecutorService service){
        return new ExecutorClient(client,service);
    }

    private final HttpClient client;
    private final ExecutorService executor;

    public ExecutorClient(final HttpClient httpClient,
                          final ExecutorService executor) {
        this.client = httpClient;
        this.executor = executor;
    }

    private <T> CompletionStage<T> get(String host, String path, Class<T>
            clazz) {

        CompletableFuture<T> promise = new CompletableFuture<>();
        executor.submit(() -> {
            try {
                HttpResponse response = client.execute(
                        new HttpGet(new URIBuilder()
                                .setScheme(SCHEME)
                                .setHost(host)
                                .setPath(VERSION + path)
                                .build().toString()));

                if (!isSuccessful(response)) {
                    promise.completeExceptionally(
                            new ExecutorClientException("Client request " +
                                    "failed status = " + response
                                    .getStatusLine().getStatusCode()));
                }

                promise.complete(
                        JsonUtils.MAPPER.readValue(
                                response.getEntity().getContent(),
                                clazz));
            } catch (Throwable t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    public CompletionStage<CassandraStatus> status(String hostname, int port) {
        return get(host(hostname, port), "/status", CassandraStatus.class);
    }

    public CompletionStage<CassandraConfig> configuration(
            String hostname,
            int port) {
        return get(host(hostname, port), "/configuration",
                CassandraConfig.class);
    }


}
