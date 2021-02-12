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
package org.projectnessie.error;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NessieError {

  private final String message;
  private final int status;
  private final String reason;
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
      @JsonProperty("status") int status,
      @JsonProperty("reason") String reason,
      @JsonProperty("serverStackTrace") String serverStackTrace) {
    this(message, status, reason, serverStackTrace, null);
  }

  /**
   * Create Error.
   *
   * @param message             Message of error.
   * @param status              Status of error.
   * @param reason              Reason for status.
   * @param serverStackTrace    Server stack trace, if available.
   * @param processingException Any processing exceptions that happened on the client.
   */
  public NessieError(String message, int status, String reason, String serverStackTrace, Exception processingException) {
    this.message = message;
    this.status = status;
    this.reason = reason;
    this.serverStackTrace = serverStackTrace;
    this.clientProcessingException = processingException;
  }

  /**
   * Create Error.
   *
   * @param statusCode          Status of error.
   * @param reason              Reason for status.
   * @param serverStackTrace    Server stack trace, if available.
   * @param processingException Any processing exceptions that happened on the client.
   */
  public NessieError(int statusCode, String reason, String serverStackTrace, Exception processingException) {
    this.status = statusCode;
    this.message = reason;
    this.reason = reason;
    this.serverStackTrace = serverStackTrace;
    this.clientProcessingException = processingException;
  }


  public String getMessage() {
    return message;
  }

  public int getStatus() {
    return status;
  }

  public String getReason() {
    return reason;
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
   *
   * @return Full error message.
   */
  @JsonIgnore
  public String getFullMessage() {
    StringBuilder sb = new StringBuilder();
    if (reason != null) {
      sb.append(reason)
        .append(" (HTTP/").append(status).append(')');
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NessieError error = (NessieError) o;
    return status == error.status && Objects.equals(message, error.message) && Objects
      .equals(serverStackTrace, error.serverStackTrace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message, status, serverStackTrace);
  }
}
