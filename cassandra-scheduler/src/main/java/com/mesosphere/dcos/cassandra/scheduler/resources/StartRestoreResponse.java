package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StartRestoreResponse {
    @JsonProperty("status")
    private String status;

    @JsonProperty("message")
    private String message;

    public StartRestoreResponse(String status, String message) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
