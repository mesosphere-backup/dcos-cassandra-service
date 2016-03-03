package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StartBackupResponse {
    @JsonProperty("status")
    private String status;

    @JsonProperty("message")
    private String message;

    public StartBackupResponse(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
}
