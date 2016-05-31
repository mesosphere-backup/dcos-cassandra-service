package com.mesosphere.dcos.cassandra.executor.backup.azure;

import java.io.IOException;

/**
 */
public class AzureIOException extends IOException {

  public AzureIOException(String message) {
    super(message);
  }

  public AzureIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public AzureIOException(Throwable cause) {
    super(cause);
  }
}
