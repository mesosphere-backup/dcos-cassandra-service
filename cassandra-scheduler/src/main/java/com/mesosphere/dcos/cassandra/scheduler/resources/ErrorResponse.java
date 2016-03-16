package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.util.Objects;

public class ErrorResponse {

    @JsonCreator
    public static ErrorResponse fromString(final String message) {
        return new ErrorResponse(message);
    }

    public static ErrorResponse fromThrowable(final Throwable throwable) {
        return fromString(throwable.getMessage());
    }

    @JsonProperty("message")
    private final String message;

    public ErrorResponse(final String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ErrorResponse)) return false;
        ErrorResponse that = (ErrorResponse) o;
        return Objects.equals(getMessage(), that.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMessage());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
