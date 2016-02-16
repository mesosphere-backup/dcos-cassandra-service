package com.mesosphere.dcos.cassandra.common.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.protocol.HTTP;
import org.apache.mesos.scheduler.plan.Completable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Created by kowens on 2/10/16.
 */
public class AsyncClientRequest implements Runnable, ResponseHandler<Object> {

    private static final Object COMPLETE = new Object();
    final HttpClient client;

    final HttpUriRequest request;

    final CompletableFuture<HttpResponse> promise =
            new CompletableFuture<>();

    public static AsyncClientRequest create(
            final HttpClient client,
            final HttpUriRequest request
            ){

        return new AsyncClientRequest(client,request);
    }

    public AsyncClientRequest(HttpClient client,
                              HttpUriRequest request) {
        this.client = client;
        this.request = request;;
    }

    public CompletionStage<HttpResponse> getFuture(){

        return promise;
    }

    public void run(){
        try{
            client.execute(request,this);
        } catch(Throwable t){

        }
    }

    @Override
    public Object handleResponse(HttpResponse httpResponse){

        promise.complete(httpResponse);

        return COMPLETE;
    }
}
