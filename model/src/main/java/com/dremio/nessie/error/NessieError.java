/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.error;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NessieError {

  private final String message;
  private final Status status;
  private final String serverStackTrace;
  private final Exception clientProcessingException;

  /**
   * Deserialize error message from the server.
   *
   * @param message Error message
   * @param status HTTP status code
   * @param serverStackTrace exception, if present (can be empty or {@code null}
   */
  @JsonCreator
  public NessieError(
      @JsonProperty("message") String message,
      @JsonProperty("status") Status status,
      @JsonProperty("serverStackTrace") String serverStackTrace) {
    this(message, status, serverStackTrace, null);
  }

  /**
   * Create Error.
   * @param message Message of error.
   * @param status Status of error.
   * @param serverStackTrace Server stack trace, if available.
   * @param processingException Any processing exceptions that happened on the client.
   */
  public NessieError(String message, Status status, String serverStackTrace, Exception processingException) {
    super();
    this.message = message;
    this.status = status;
    this.serverStackTrace = serverStackTrace;
    this.clientProcessingException = processingException;
  }

  public String getMessage() {
    return message;
  }

  public Status getStatus() {
    return status;
  }

  public String getServerStackTrace() {
    return serverStackTrace;
  }

  @JsonIgnore
  public Exception getClientProcessingException() {
    return clientProcessingException;
  }

  /**
   * Get full error message.
   * @return Full error message.
   */
  @JsonIgnore
  public String getFullMessage() {
    StringBuilder sb = new StringBuilder();
    if (status != null) {
      sb.append(status.getReasonPhrase())
        .append(" (HTTP/").append(status.getStatusCode()).append(')');
    }

    if (message != null) {
      if (sb.length() > 0) {
        sb.append(": ");
      }
      sb.append(message);
    }

    if (serverStackTrace != null) {
      sb.append("\n").append(serverStackTrace);
    }
    if (clientProcessingException != null) {
      StringWriter sw = new StringWriter();
      clientProcessingException.printStackTrace(new PrintWriter(sw));
      sb.append("\n").append(sw);
    }
    return sb.toString();
  }
}
